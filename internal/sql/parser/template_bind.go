package parser

import (
	"fmt"

	"github.com/steamwo/fssql/internal/sql/ast"
)

func bindTemplateSelect(query *ast.SelectStmt, values []ast.Expr) (*ast.SelectStmt, error) {
	return (&templateBinder{values: values}).selectQuery(query)
}

type templateBinder struct{ values []ast.Expr }

func(b *templateBinder)statement(stmt ast.Statement)(ast.Statement,error){
	switch s:=stmt.(type){
	case *ast.CreateSecretStmt:
		out:=*s;out.Options=append([]ast.SecretOption(nil),s.Options...);for i:=range out.Options{if raw:=out.Options[i].StringParameter;raw!=nil{value,err:=b.expr(raw);if err!=nil{return nil,err};lit,ok:=unwrapParameter(value).(*ast.LiteralExpr);if !ok||lit.Kind!=ast.LiteralString{return nil,fmt.Errorf("secret value parameters must be VARCHAR")};out.Options[i].StringValue=lit;out.Options[i].StringParameter=nil}};return &out,nil
	case *ast.SetStmt:out:=*s;var err error;out.Value,err=b.expr(s.Value);out.Value=unwrapParameter(out.Value);return &out,err
	case *ast.SelectStmt:return b.selectQuery(s)
	case *ast.CreateViewStmt:out:=*s;var err error;out.Query,err=b.selectQuery(s.Query);if err!=nil{return nil,err};out.BoundSQL,err=ast.FormatSelect(out.Query);return &out,err
	case *ast.CreateTableAsStmt:out:=*s;var err error;out.Query,err=b.selectQuery(s.Query);return &out,err
	case *ast.DescribeStmt:out:=*s;var err error;out.Query,err=b.selectQuery(s.Query);return &out,err
	case *ast.CopyStmt:
		out:=*s;var err error;out.Query,err=b.selectQuery(s.Query);if err!=nil{return nil,err}
		if s.DestinationParameter!=nil{v,err:=b.expr(s.DestinationParameter);if err!=nil{return nil,err};lit,ok:=unwrapParameter(v).(*ast.LiteralExpr);if !ok||lit.Kind!=ast.LiteralString{return nil,fmt.Errorf("COPY destination parameter must be VARCHAR")};out.Destination=lit;out.DestinationParameter=nil}
		out.Options=append([]ast.CopyOption(nil),s.Options...);for i:=range out.Options{out.Options[i].Value,err=b.expr(out.Options[i].Value);if err!=nil{return nil,err};out.Options[i].Value=unwrapParameter(out.Options[i].Value)};return &out,nil
	case *ast.CreateExternalTableStmt:
		out:=*s;if s.SourceParameter!=nil{v,err:=b.expr(s.SourceParameter);if err!=nil{return nil,err};lit,ok:=unwrapParameter(v).(*ast.LiteralExpr);if !ok||lit.Kind!=ast.LiteralString{return nil,fmt.Errorf("external table source parameter must be VARCHAR")};out.Source=lit;out.SourceParameter=nil};return &out,nil
	default:return nil,fmt.Errorf("sql parameter binding is not supported for %T",stmt)
	}
}

func (b *templateBinder) selectQuery(query *ast.SelectStmt) (*ast.SelectStmt, error) {
	if query == nil {
		return nil, nil
	}
	out := *query
	var err error
	out.UnionLeft, err = b.selectQuery(query.UnionLeft)
	if err != nil {
		return nil, err
	}
	out.UnionRight, err = b.selectQuery(query.UnionRight)
	if err != nil {
		return nil, err
	}
	if query.With != nil {
		with := *query.With
		with.CTEs = append([]ast.CTE(nil), query.With.CTEs...)
		for i := range with.CTEs {
			with.CTEs[i].Query, err = b.selectQuery(with.CTEs[i].Query)
			if err != nil {
				return nil, err
			}
		}
		out.With = &with
	}
	out.From, err = b.table(query.From)
	if err != nil {
		return nil, err
	}
	out.Projection = append([]ast.SelectItem(nil), query.Projection...)
	for i := range out.Projection {
		out.Projection[i].Expr, err = b.expr(query.Projection[i].Expr)
		if err != nil {
			return nil, err
		}
	}
	out.Where, err = b.expr(query.Where)
	if err != nil {
		return nil, err
	}
	out.GroupBy = make([]ast.Expr, len(query.GroupBy))
	for i := range out.GroupBy {
		out.GroupBy[i], err = b.expr(query.GroupBy[i])
		if err != nil {
			return nil, err
		}
	}
	out.Having, err = b.expr(query.Having)
	if err != nil {
		return nil, err
	}
	out.OrderBy = append([]ast.OrderTerm(nil), query.OrderBy...)
	for i := range out.OrderBy {
		out.OrderBy[i].Expr, err = b.expr(query.OrderBy[i].Expr)
		if err != nil {
			return nil, err
		}
	}
	out.Limit, err = b.expr(query.Limit)
	if err != nil {
		return nil, err
	}
	out.Offset, err = b.expr(query.Offset)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (b *templateBinder) expr(expr ast.Expr) (ast.Expr, error) {
	switch e := expr.(type) {
	case nil:
		return nil, nil
	case *ast.ParameterExpr:
		if e.Slot < 0 || e.Slot >= len(b.values) || b.values[e.Slot] == nil {
			return nil, fmt.Errorf("sql parameter: invalid value slot")
		}
		out := *e
		out.Value = b.values[e.Slot]
		return &out, nil
	case *ast.LiteralExpr, *ast.NameExpr:
		return expr, nil
	case *ast.UnaryExpr:
		out := *e
		var err error
		out.Expr, err = b.expr(e.Expr)
		return &out, err
	case *ast.BinaryExpr:
		out := *e
		var err error
		out.Left, err = b.expr(e.Left)
		if err != nil {
			return nil, err
		}
		out.Right, err = b.expr(e.Right)
		return &out, err
	case *ast.CallExpr:
		out := *e
		out.Args = make([]ast.Expr, len(e.Args))
		for i := range out.Args {
			var err error
			out.Args[i], err = b.expr(e.Args[i])
			if err != nil {
				return nil, err
			}
		}
		return &out, nil
	case *ast.CopyEncryptionExpr:
		out:=*e;out.Fields=append([]ast.CopyOption(nil),e.Fields...);for i:=range out.Fields{var err error;out.Fields[i].Value,err=b.expr(out.Fields[i].Value);if err!=nil{return nil,err};out.Fields[i].Value=unwrapParameter(out.Fields[i].Value)};return &out,nil
	case *ast.IsNullExpr:
		out := *e
		var err error
		out.Expr, err = b.expr(e.Expr)
		return &out, err
	case *ast.SubqueryExpr:
		out := *e
		var err error
		out.Query, err = b.selectQuery(e.Query)
		return &out, err
	case *ast.InSubqueryExpr:
		out := *e
		var err error
		out.Value, err = b.expr(e.Value)
		if err != nil {
			return nil, err
		}
		out.Query, err = b.selectQuery(e.Query)
		return &out, err
	default:
		return nil, fmt.Errorf("sql parameter: unsupported expression %T", expr)
	}
}

func (b *templateBinder) table(table ast.TableExpr) (ast.TableExpr, error) {
	switch t := table.(type) {
	case nil:
		return nil, nil
	case *ast.TableName:
		return table, nil
	case *ast.DerivedTable:
		out := *t
		var err error
		out.Query, err = b.selectQuery(t.Query)
		return &out, err
	case *ast.SampleTableExpr:
		out := *t
		var err error
		out.Input, err = b.table(t.Input)
		return &out, err
	case *ast.TableFunction:
		out := *t
		expr, err := b.expr(t.Call)
		if err != nil {
			return nil, err
		}
		out.Call = expr.(*ast.CallExpr)
		for i, arg := range out.Call.Args {
			out.Call.Args[i] = unwrapParameter(arg)
			if option, ok := out.Call.Args[i].(*ast.BinaryExpr); ok {
				option.Right = unwrapParameter(option.Right)
			}
		}
		if err := foldStaticTableArguments(out.Call); err != nil {
			return nil, err
		}
		return &out, nil
	case *ast.JoinExpr:
		out := *t
		var err error
		out.Left, err = b.table(t.Left)
		if err != nil {
			return nil, err
		}
		out.Right, err = b.table(t.Right)
		if err != nil {
			return nil, err
		}
		out.Condition, err = b.expr(t.Condition)
		return &out, err
	default:
		return nil, fmt.Errorf("sql parameter: unsupported table expression %T", table)
	}
}

func unwrapParameter(expr ast.Expr) ast.Expr {
	if parameter, ok := expr.(*ast.ParameterExpr); ok {
		return parameter.Value
	}
	return expr
}
