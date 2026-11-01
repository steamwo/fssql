package sort

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/steamwo/fssql/internal/core/memory"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
)

type runReader struct { sorter *ExternalSort; run *run; page int; buf *memory.Buffer; pos int; row []byte; done bool }
func(r *runReader)loadPage(ctx context.Context)error{if r.buf!=nil{r.buf.Release();r.buf=nil};if r.page>=r.run.pages.Count(){r.done=true;r.row=nil;return nil};id,_:=r.run.pages.At(r.page);b,err:=r.sorter.store.ReadPage(ctx,id);if err!=nil{return err};r.buf=b;r.pos=0;r.page++;return nil}
func(r *runReader)advance(ctx context.Context)error{if r.done{return nil};for{if r.buf==nil||r.pos>=r.buf.Len(){if err:=r.loadPage(ctx);err!=nil{return err};if r.done{return nil}};if r.buf.Len()-r.pos<4{return errors.New("truncated sort run page")};n:=int(binary.LittleEndian.Uint32(r.buf.Bytes()[r.pos:r.pos+4]));r.pos+=4;if n<sortMinRow(r.sorter.layout)||n>r.buf.Len()-r.pos{return errors.New("invalid sort row length")};r.row=r.buf.Bytes()[r.pos:r.pos+n];r.pos+=n;return nil}}
func(r *runReader)close(){if r.buf!=nil{r.buf.Release();r.buf=nil}}
func sortMinRow(l exechash.RowLayout)int{return l.FixedWidth}

type readerHeap struct{sorter *ExternalSort;readers []*runReader;buf *memory.Buffer;n int}
func newReaderHeap(s *ExternalSort,rs []*runReader)(*readerHeap,error){b,err:=s.cfg.Buffers.Allocate(s.cfg.Scope,memory.TagSort,len(rs)*4);if err!=nil{return nil,err};h:=&readerHeap{sorter:s,readers:rs,buf:b};for i,r:=range rs{if !r.done{h.set(h.n,i);h.n++}};for i:=h.n/2-1;i>=0;i--{if err:=h.down(i);err!=nil{b.Release();return nil,err}};return h,nil}
func(h *readerHeap)get(i int)int{return int(binary.LittleEndian.Uint32(h.buf.Bytes()[i*4:i*4+4]))}
func(h *readerHeap)set(i,v int){binary.LittleEndian.PutUint32(h.buf.Bytes()[i*4:i*4+4],uint32(v))}
func(h *readerHeap)swap(i,j int){a,b:=h.get(i),h.get(j);h.set(i,b);h.set(j,a)}
func(h *readerHeap)less(i,j int)(bool,error){c,err:=h.sorter.compareRows(h.readers[h.get(i)].row,h.readers[h.get(j)].row);if err!=nil{return false,err};if c==0{return h.get(i)<h.get(j),nil};return c<0,nil}
func(h *readerHeap)down(i int)error{for{l:=2*i+1;if l>=h.n{return nil};best:=l;if l+1<h.n{ok,err:=h.less(l+1,l);if err!=nil{return err};if ok{best=l+1}};ok,err:=h.less(best,i);if err!=nil{return err};if !ok{return nil};h.swap(i,best);i=best}}
func(h *readerHeap)top()int{return h.get(0)}
func(h *readerHeap)fixTop()error{return h.down(0)}
func(h *readerHeap)popTop()error{h.n--;if h.n>0{h.set(0,h.get(h.n));return h.down(0)};return nil}
func(h *readerHeap)release(){if h.buf!=nil{h.buf.Release();h.buf=nil}}
func(s *ExternalSort)mergeRuns(ctx context.Context,runs []*run,consume func([]byte)error)error{rs:=make([]*runReader,len(runs));for i,r:=range runs{rr:=&runReader{sorter:s,run:r};if err:=rr.advance(ctx);err!=nil{for _,x:=range rs{if x!=nil{x.close()}};return err};rs[i]=rr};h,err:=newReaderHeap(s,rs);if err!=nil{for _,r:=range rs{r.close()};return err};defer h.release();defer func(){for _,r:=range rs{r.close()}}();for h.n>0{if err:=ctx.Err();err!=nil{return err};ri:=h.top();rr:=rs[ri];if err:=consume(rr.row);err!=nil{return err};if err:=rr.advance(ctx);err!=nil{return err};if rr.done{if err:=h.popTop();err!=nil{return err}}else{if err:=h.fixTop();err!=nil{return err}}};return nil}
func(s *ExternalSort)maxFanIn()int{per:=int64(s.cfg.SpillBlockBytes);if per<512{per=512};n:=int(s.cfg.MemoryBudget/per)-1;if n<2{n=2};if n>64{n=64};return n}
func(s *ExternalSort)deleteRun(r *run)error{if r==nil||r.pages==nil{return nil};for i:=0;i<r.pages.Count();i++{id,_:=r.pages.At(i);if err:=s.store.DeletePage(id);err!=nil{return err}};r.pages.Release();r.pages=nil;return nil}
func(s *ExternalSort)mergeGroupToRun(ctx context.Context,group []*run)(*run,error){w,err:=newRunWriter(s.store,s.cfg.Scope,s.cfg.Buffers,s.cfg.SpillBlockBytes);if err!=nil{return nil,err};if err:=s.mergeRuns(ctx,group,func(row []byte)error{return w.append(ctx,row)});err!=nil{w.release();return nil,err};r,err:=w.close(ctx);if err!=nil{w.release();return nil,err};for _,old:=range group{if err:=s.deleteRun(old);err!=nil{if r.pages!=nil{r.pages.Release()};return nil,err}};s.stats.MergePasses++;s.stats.RunsCreated++;s.stats.SpillPages+=r.pages.Count();return r,nil}
func(s *ExternalSort)compactRuns(ctx context.Context)error{fan:=s.maxFanIn();for len(s.runs)>fan{var next []*run;for i:=0;i<len(s.runs);i+=fan{end:=i+fan;if end>len(s.runs){end=len(s.runs)};if end-i==1{next=append(next,s.runs[i]);continue};r,err:=s.mergeGroupToRun(ctx,s.runs[i:end]);if err!=nil{return err};next=append(next,r)};s.runs=next;if err:=s.runMeta.Resize(int64(len(s.runs)*64));err!=nil{return err}};return nil}
