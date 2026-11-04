package aggregate

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/vector"
)

func(e *ExternalAggregate)repartitionPartition(ctx context.Context,input aggregatePartition,depth int)([]aggregatePartition,error){if depth>e.cfg.MaxDepth{return nil,fmt.Errorf("aggregate repartition depth %d exceeds max %d",depth,e.cfg.MaxDepth)};children:=make([]aggregatePartition,e.radix.Count());cleanup:=func(){for i:=range children{_ = e.deletePartition(children[i])}};for child:=0;child<e.radix.Count();child++{w,err:=newAggregateSpillWriter(e.store,e.cfg.Scope,e.cfg.Buffers,e.cfg.SpillBlockBytes);if err!=nil{cleanup();return nil,err};for i:=0;i<input.pages.Count();i++{if err:=ctx.Err();err!=nil{w.release();cleanup();return nil,err};id,_:=input.pages.At(i);buf,err:=e.store.ReadPage(ctx,id);if err!=nil{w.release();cleanup();return nil,err};err=scanAggregateRecords(buf.Bytes(),func(rec []byte)error{h:=binary.LittleEndian.Uint64(rec[:8]);part,err:=e.radix.PartitionAtDepth(h,depth);if err!=nil{return err};if part!=child{return nil};return w.append(ctx,rec)});buf.Release();if err!=nil{w.release();cleanup();return nil,err}};if err:=w.close(ctx);err!=nil{w.release();cleanup();return nil,err};if w.rows>0{children[child]=aggregatePartition{pages:w.pages,rows:w.rows};e.stats.SpillPages+=w.pages.Count();e.stats.SpilledGroups+=w.rows;w.pages=nil};w.release()};if err:=e.deletePartition(input);err!=nil{cleanup();return nil,err};e.stats.Repartitions++;return children,nil}
func(e *ExternalAggregate)processPartition(ctx context.Context,part aggregatePartition,depth int,emit func(*vector.DataChunk)error)error{if part.pages==nil{return nil};if e.local!=nil{e.local.Release();e.local=nil};if err:=e.resetLocal();err!=nil{return err};for i:=0;i<part.pages.Count();i++{if err:=ctx.Err();err!=nil{return err};id,_:=part.pages.At(i);buf,err:=e.store.ReadPage(ctx,id);if err!=nil{return err};err=scanAggregateRecords(buf.Bytes(),func(rec []byte)error{if err:=e.applyRecord(ctx,rec);err!=nil{return err};e.updatePeak(e.local.ManagedBytes());return nil});buf.Release();if err!=nil{return err};if e.local.ManagedBytes()>e.cfg.MemoryBudget&&e.local.GroupCount()>1{if depth>=e.cfg.MaxDepth{return fmt.Errorf("aggregate partition remains above memory budget at max repartition depth %d",e.cfg.MaxDepth)};e.local.Release();e.local=nil;children,err:=e.repartitionPartition(ctx,part,depth+1);if err!=nil{return err};for _,child:=range children{if err:=e.processPartition(ctx,child,depth+1,emit);err!=nil{return err}};return nil}};if err:=e.local.Emit(ctx,emit);err!=nil{return err};return e.deletePartition(part)}
func(e *ExternalAggregate)Emit(ctx context.Context,emit func(*vector.DataChunk)error)error{
	if emit==nil{return errors.New("emit callback required")}
	if err:=ctx.Err();err!=nil{return err}
	e.mergeMu.Lock();defer e.mergeMu.Unlock()
	e.shardGate.Lock();defer e.shardGate.Unlock()
	if e.finalized{return errors.New("external aggregate finalized")}
	if e.spilled{if err:=e.spillShardsLocked(ctx);err!=nil{return err}}else{if err:=e.mergeLocked(ctx);err!=nil{return err}}
	if !e.spilled{err:=e.local.Emit(ctx,emit);if err==nil{e.resetShardsToLocal();e.finalized=true};return err}
	for p:=range e.partitions{part:=e.partitions[p];e.partitions[p]=aggregatePartition{};if err:=e.processPartition(ctx,part,0,emit);err!=nil{return err}}
	e.resetShardsToLocal()
	e.finalized=true
	return nil
}
func(e *ExternalAggregate)Release(){
	e.mergeMu.Lock()
	e.shardGate.Lock()
	e.shardMu.Lock()
	for _,shard:=range e.shards{if shard!=nil{shard.Release()}}
	e.shards=nil;e.shardBytes=nil;e.totalBytes=0
	e.shardMu.Unlock()
	if e.local!=nil{e.local.Release();e.local=nil}
	for i:=range e.partitions{if e.partitions[i].pages!=nil{e.partitions[i].pages.Release();e.partitions[i].pages=nil}}
	if e.recordScratch!=nil{e.recordScratch.Release();e.recordScratch=nil}
	if e.store!=nil{_ = e.store.Close();e.store=nil}
	e.shardGate.Unlock()
	e.mergeMu.Unlock()
}
