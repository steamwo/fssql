package planner

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func PreparePhase5BoundQuery(q *binder.BoundQuery) error {
	seen := map[*binder.BoundQuery]bool{}
	var visitRelation func(*binder.BoundRelation) error
	var visitQuery func(*binder.BoundQuery) error
	visitQuery = func(query *binder.BoundQuery) error {
		if query == nil || seen[query] { return nil }
		seen[query] = true
		if query.Source != nil { return visitRelation(query.Source) }
		return nil
	}
	visitRelation = func(r *binder.BoundRelation) error {
		if r == nil { return nil }
		for _, child := range r.Children { if err := visitRelation(child); err != nil { return err } }
		if r.Kind != binder.RelationCTE || r.CTEQuery == nil { return nil }
		if spec, ok := sampleSpecFromCTEID(r.ID); ok {
			if !containsPhase5Marker(r.CTEQuery.Where, function.SampleMarkerID) {
				marker := phase5Marker(function.SampleMarkerID, spec)
				if r.CTEQuery.Where == nil { r.CTEQuery.Where = marker } else { r.CTEQuery.Where = phase5And(r.CTEQuery.Where, marker) }
			}
		}
		return visitQuery(r.CTEQuery)
	}
	return visitQuery(q)
}

func phase5Marker(id function.ID, literal string) *binder.BoundExpression {
	child := &binder.BoundExpression{Kind:binder.BoundLiteral, Type:types.LogicalType{ID:types.Varchar}, Literal:literal}
	return &binder.BoundExpression{Kind:binder.BoundFunction, Type:types.LogicalType{ID:types.Boolean}, FunctionID:id, FunctionKind:function.Scalar, Children:[]*binder.BoundExpression{child}}
}

func phase5And(a,b *binder.BoundExpression)*binder.BoundExpression {
	return &binder.BoundExpression{Kind:binder.BoundBinary, Type:types.LogicalType{ID:types.Boolean}, Nullable:a.Nullable||b.Nullable, Op:"AND", Children:[]*binder.BoundExpression{a,b}}
}

func containsPhase5Marker(e *binder.BoundExpression,id function.ID)bool{
	if e==nil{return false};if e.Kind==binder.BoundFunction&&e.FunctionID==id{return true};for _,c:=range e.Children{if containsPhase5Marker(c,id){return true}};return false
}

func sampleSpecFromCTEID(id string)(string,bool){
	name:=strings.TrimPrefix(strings.ToLower(id),"cte:")
	const prefix="__fssql_sample_"
	if !strings.HasPrefix(name,prefix){return "",false}
	parts:=strings.Split(strings.TrimPrefix(name,prefix),"_")
	if len(parts)<5{return "",false}
	method,value,seedKind,seed:=parts[0],parts[1],parts[2],parts[3]
	if method!="r"&&method!="b"{return "",false};if _,err:=strconv.ParseUint(value,16,64);err!=nil{return "",false}
	if seedKind!="s"&&seedKind!="n"{return "",false};if _,err:=strconv.ParseUint(seed,16,64);err!=nil{return "",false}
	return strings.Join([]string{"sample",method,value,seedKind,seed},":"),true
}

func RewritePhase5Logical(root *LogicalPlan)(*LogicalPlan,error){
	if root==nil{return nil,fmt.Errorf("phase5: nil logical plan")}
	counts:=map[string]int{}
	countPhase5RepeatedProjects(root,counts)
	return rewritePhase5Logical(root,counts)
}

func countPhase5RepeatedProjects(root *LogicalPlan,counts map[string]int){
	if root==nil{return}
	if key:=phase5ProjectIdentity(root);key!=""{counts[key]++}
	for _,child:=range root.Children{countPhase5RepeatedProjects(child,counts)}
}

func phase5ProjectIdentity(root *LogicalPlan)string{
	if root==nil||root.Kind!=LogicalProject||len(root.Projection)==0{return ""}
	var b strings.Builder
	b.WriteString("project:")
	for _,expr:=range root.Projection{fmt.Fprintf(&b,"%p,",expr)}
	return b.String()
}

func rewritePhase5Logical(root *LogicalPlan,counts map[string]int)(*LogicalPlan,error){
	if root==nil{return nil,fmt.Errorf("phase5: nil logical plan")}
	projectKey:=phase5ProjectIdentity(root)
	for i,child:=range root.Children{rewritten,err:=rewritePhase5Logical(child,counts);if err!=nil{return nil,err};root.Children[i]=rewritten}
	if root.Kind==LogicalJoin && root.Join==JoinInner && isPhase5NullSafeEquality(root.Predicate) {
		if _,err:=ExtractHashJoinKeys(root.Predicate,len(root.Children[0].Output),len(root.Children[1].Output));err==nil{return root,nil}
		predicate:=root.Predicate
		root.Join=JoinCross
		root.Predicate=nil
		return &LogicalPlan{Kind:LogicalFilter,Children:[]*LogicalPlan{root},Output:clonePlannerColumns(root.Output),Predicate:predicate},nil
	}
	if root.Kind==LogicalFilter {
		residual,spec,ok,err:=stripSampleMarker(root.Predicate);if err!=nil{return nil,err};if ok{
			child:=root.Children[0]
			if residual!=nil{child=&LogicalPlan{Kind:LogicalFilter,Children:[]*LogicalPlan{child},Output:clonePlannerColumns(child.Output),Predicate:residual}}
			marker:=phase5Marker(function.SampleMarkerID,spec)
			return &LogicalPlan{Kind:LogicalSort,Children:[]*LogicalPlan{child},Output:clonePlannerColumns(child.Output),Sort:[]SortKey{{Expr:marker}}},nil
		}
	}
	if root.Kind==LogicalProject {
		rewritten,err:=rewritePhase5WindowProject(root);if err!=nil{return nil,err}
		if projectKey!=""&&counts[projectKey]>1{
			marker:=phase5Marker(function.CTEMarkerID,projectKey)
			return &LogicalPlan{Kind:LogicalSort,Children:[]*LogicalPlan{rewritten},Output:clonePlannerColumns(rewritten.Output),Sort:[]SortKey{{Expr:marker}}},nil
		}
		return rewritten,nil
	}
	return root,nil
}

func isPhase5NullSafeEquality(e *binder.BoundExpression) bool {
	// A null-safe conjunct requires the same residual execution as a single key.
	if e != nil && e.Kind == binder.BoundBinary && e.Op == "AND" && len(e.Children) == 2 {
		return isPhase5NullSafeEquality(e.Children[0]) || isPhase5NullSafeEquality(e.Children[1])
	}
	if e==nil || e.Kind!=binder.BoundBinary || e.Op!="OR" || len(e.Children)!=2 { return false }
	coalesce,bothNull:=e.Children[0],e.Children[1]
	if coalesce==nil || coalesce.Kind!=binder.BoundFunction || coalesce.FunctionID!=function.CoalesceID || len(coalesce.Children)!=2 { return false }
	eq,falseLit:=coalesce.Children[0],coalesce.Children[1]
	if eq==nil || eq.Kind!=binder.BoundBinary || eq.Op!="=" || len(eq.Children)!=2 || falseLit==nil || falseLit.Kind!=binder.BoundLiteral || !strings.EqualFold(falseLit.Literal,"FALSE") { return false }
	if eq.Children[0]==nil || eq.Children[1]==nil || eq.Children[0].Kind!=binder.BoundColumn || eq.Children[1].Kind!=binder.BoundColumn { return false }
	if bothNull==nil || bothNull.Kind!=binder.BoundBinary || bothNull.Op!="AND" || len(bothNull.Children)!=2 { return false }
	for _,child:=range bothNull.Children { if child==nil || child.Kind!=binder.BoundIsNull || child.Op!="IS NULL" || len(child.Children)!=1 || child.Children[0]==nil || child.Children[0].Kind!=binder.BoundColumn { return false } }
	return bothNull.Children[0].Children[0].ColumnIndex==eq.Children[0].ColumnIndex && bothNull.Children[1].Children[0].ColumnIndex==eq.Children[1].ColumnIndex
}

func stripSampleMarker(e *binder.BoundExpression)(*binder.BoundExpression,string,bool,error){
	if e==nil{return nil,"",false,nil}
	if e.Kind==binder.BoundFunction&&e.FunctionID==function.SampleMarkerID{
		if len(e.Children)!=1||e.Children[0].Kind!=binder.BoundLiteral{return nil,"",false,fmt.Errorf("phase5: malformed sample marker")}
		return nil,e.Children[0].Literal,true,nil
	}
	if e.Kind==binder.BoundBinary&&strings.EqualFold(e.Op,"AND")&&len(e.Children)==2{
		l,ls,lo,err:=stripSampleMarker(e.Children[0]);if err!=nil{return nil,"",false,err};r,rs,ro,err:=stripSampleMarker(e.Children[1]);if err!=nil{return nil,"",false,err}
		if lo&&ro{return nil,"",false,fmt.Errorf("phase5: multiple sample markers")};spec:=ls;if ro{spec=rs};if !lo&&!ro{return e,"",false,nil};if l==nil{return r,spec,true,nil};if r==nil{return l,spec,true,nil};return phase5And(l,r),spec,true,nil
	}
	return e,"",false,nil
}

type phase5WindowSpec struct{ partition,order int; desc string; nulls string; frame string }
func parseWindowSpec(e *binder.BoundExpression)(phase5WindowSpec,error){
	if e==nil||len(e.Children)<1||e.Children[0].Kind!=binder.BoundLiteral{return phase5WindowSpec{},fmt.Errorf("phase5: malformed window specification")}
	var s phase5WindowSpec
	for _,part:=range strings.Split(e.Children[0].Literal,";"){kv:=strings.SplitN(part,"=",2);if len(kv)!=2{continue};switch kv[0]{case"p":s.partition,_=strconv.Atoi(kv[1]);case"o":s.order,_=strconv.Atoi(kv[1]);case"d":s.desc=kv[1];case"n":s.nulls=kv[1];case"f":s.frame=kv[1]}}
	if s.partition<0||s.order<0||len(s.desc)!=s.order||len(s.nulls)!=s.order{return s,fmt.Errorf("phase5: invalid window specification %q",e.Children[0].Literal)}
	return s,nil
}

func collectPhase5Windows(e *binder.BoundExpression,out *[]*binder.BoundExpression,seen map[*binder.BoundExpression]bool){
	if e==nil{return}
	if e.Kind==binder.BoundFunction&&function.IsWindowID(e.FunctionID){if !seen[e]{seen[e]=true;*out=append(*out,e)};return}
	for _,child:=range e.Children{collectPhase5Windows(child,out,seen)}
}
func phase5PassThrough(columns []Column)[]*binder.BoundExpression{
	out:=make([]*binder.BoundExpression,len(columns))
	for i,column:=range columns{out[i]=&binder.BoundExpression{Kind:binder.BoundColumn,Type:column.Type,Nullable:column.Nullable,ColumnIndex:i}}
	return out
}
func phase5ReplaceWindows(e *binder.BoundExpression,replacements map[*binder.BoundExpression]*binder.BoundExpression)*binder.BoundExpression{
	if e==nil{return nil}
	if replacement,ok:=replacements[e];ok{return replacement}
	out:=*e
	if len(e.Children)>0{out.Children=make([]*binder.BoundExpression,len(e.Children));for i,child:=range e.Children{out.Children[i]=phase5ReplaceWindows(child,replacements)}}
	return &out
}
func phase5WindowColumn(w *binder.BoundExpression,index int)Column{return Column{Name:fmt.Sprintf("__phase5_window_%d",index),Type:w.Type,Nullable:w.Nullable}}

func rewritePhase5WindowProject(project *LogicalPlan)(*LogicalPlan,error){
	if len(project.Children)!=1{return nil,fmt.Errorf("phase5: window project requires one child")}
	windows:=make([]*binder.BoundExpression,0)
	seen:=map[*binder.BoundExpression]bool{}
	for _,e:=range project.Projection{collectPhase5Windows(e,&windows,seen)}
	if len(windows)==0{return project,nil}
	child:=project.Children[0]
	globals:=make([]*binder.BoundExpression,0)
	stateful:=make([]*binder.BoundExpression,0)
	for _,w:=range windows{spec,err:=parseWindowSpec(w);if err!=nil{return nil,err};if (w.FunctionID==function.WindowCountID||w.FunctionID==function.WindowSumID)&&spec.partition==0&&spec.order==0&&spec.frame=="none"{globals=append(globals,w)}else{stateful=append(stateful,w)}}
	replacements:=make(map[*binder.BoundExpression]*binder.BoundExpression,len(windows))

	if len(globals)>0{
		globalChild:=child
		projection:=phase5PassThrough(child.Output)
		materializedOut:=clonePlannerColumns(child.Output)
		specs:=make([]string,len(globals))
		for i,w:=range globals{
			if w.FunctionID==function.WindowCountID{specs[i]="c";continue}
			if len(w.Children)<2{return nil,fmt.Errorf("phase5: SUM window is missing its value")}
			input:=len(materializedOut)
			projection=append(projection,w.Children[1])
			materializedOut=append(materializedOut,Column{Name:fmt.Sprintf("__phase5_global_arg_%d",i),Type:w.Children[1].Type,Nullable:w.Children[1].Nullable})
			specs[i]="s:"+strconv.Itoa(input)
		}
		if len(materializedOut)!=len(child.Output){globalChild=&LogicalPlan{Kind:LogicalProject,Children:[]*LogicalPlan{child},Output:materializedOut,Projection:projection}}
		base:=len(globalChild.Output)
		globalOut:=clonePlannerColumns(globalChild.Output)
		for i,w:=range globals{column:=phase5WindowColumn(w,base+i);globalOut=append(globalOut,column);replacements[w]=&binder.BoundExpression{Kind:binder.BoundColumn,Type:w.Type,Nullable:w.Nullable,ColumnIndex:base+i}}
		marker:=phase5Marker(function.GlobalWindowMarkerID,strings.Join(specs,";"))
		child=&LogicalPlan{Kind:LogicalSort,Children:[]*LogicalPlan{globalChild},Output:globalOut,Sort:[]SortKey{{Expr:marker}}}
	}

	for _,w:=range stateful{
		spec,err:=parseWindowSpec(w);if err!=nil{return nil,err}
		valueOffset:=1;if w.FunctionID==function.WindowSumID{valueOffset=2}
		need:=spec.partition+spec.order
		if len(w.Children)<valueOffset+need{return nil,fmt.Errorf("phase5: incomplete window arguments")}
		keys:=make([]SortKey,0,need)
		for k:=0;k<need;k++{expr:=w.Children[valueOffset+k];if expr==nil||expr.Kind!=binder.BoundColumn{return nil,fmt.Errorf("phase5: window partition/order expressions must be materialized columns")};key:=SortKey{Expr:expr};if k>=spec.partition{oi:=k-spec.partition;key.Descending=spec.desc[oi]=='1';nf:=spec.nulls[oi]=='f';key.NullsFirst=&nf};keys=append(keys,key)}
		if len(keys)>0{child=&LogicalPlan{Kind:LogicalSort,Children:[]*LogicalPlan{child},Output:clonePlannerColumns(child.Output),Sort:keys}}
		base:=len(child.Output)
		stageProjection:=phase5PassThrough(child.Output)
		stageProjection=append(stageProjection,w)
		stageOut:=clonePlannerColumns(child.Output)
		column:=phase5WindowColumn(w,base)
		stageOut=append(stageOut,column)
		child=&LogicalPlan{Kind:LogicalProject,Children:[]*LogicalPlan{child},Output:stageOut,Projection:stageProjection}
		replacements[w]=&binder.BoundExpression{Kind:binder.BoundColumn,Type:w.Type,Nullable:w.Nullable,ColumnIndex:base}
	}

	for i,e:=range project.Projection{project.Projection[i]=phase5ReplaceWindows(e,replacements)}
	project.Children[0]=child
	return project,nil
}

func clonePhase5Logical(p *LogicalPlan)*LogicalPlan{if p==nil{return nil};out:=*p;out.Output=clonePlannerColumns(p.Output);out.Projection=append([]*binder.BoundExpression(nil),p.Projection...);out.GroupBy=append([]*binder.BoundExpression(nil),p.GroupBy...);out.Aggregates=append([]AggregateSpec(nil),p.Aggregates...);out.Sort=append([]SortKey(nil),p.Sort...);out.Children=make([]*LogicalPlan,len(p.Children));for i,c:=range p.Children{out.Children[i]=clonePhase5Logical(c)};if p.Scan!=nil{s:=*p.Scan;s.Pushdown.Projection=append([]int(nil),p.Scan.Pushdown.Projection...);s.Pushdown.Filters=append([]*binder.BoundExpression(nil),p.Scan.Pushdown.Filters...);out.Scan=&s};return &out}

func IsPhase5SampleSort(keys []SortKey)(string,bool){if len(keys)!=1||keys[0].Expr==nil||keys[0].Expr.Kind!=binder.BoundFunction||keys[0].Expr.FunctionID!=function.SampleMarkerID||len(keys[0].Expr.Children)!=1{return "",false};return keys[0].Expr.Children[0].Literal,true}
func IsPhase5CTESort(keys []SortKey)(string,bool){if len(keys)!=1||keys[0].Expr==nil||keys[0].Expr.Kind!=binder.BoundFunction||keys[0].Expr.FunctionID!=function.CTEMarkerID||len(keys[0].Expr.Children)!=1{return "",false};return keys[0].Expr.Children[0].Literal,true}
func IsPhase5GlobalWindowSort(keys []SortKey)(string,bool){if len(keys)!=1||keys[0].Expr==nil||keys[0].Expr.Kind!=binder.BoundFunction||keys[0].Expr.FunctionID!=function.GlobalWindowMarkerID||len(keys[0].Expr.Children)!=1{return "",false};return keys[0].Expr.Children[0].Literal,true}
