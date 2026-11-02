package pipeline

import "errors"

// AbortUnstarted 回收已经构造但尚未进入 RunDAG 的 Pipeline tree。
// 它不调用 Sink.Finalize；仅关闭 dependency、Abort hook、CloseableSource 与 CloseableSink ownership。
// 调用方必须保证该 tree 尚未执行，避免与 scheduler task 并发关闭。
func AbortUnstarted(root *Pipeline) error {
	visited := map[*Pipeline]bool{}
	var abort func(*Pipeline) error
	abort = func(p *Pipeline) error {
		if p == nil || visited[p] {
			return nil
		}
		visited[p] = true
		var errs []error
		for _, dependency := range p.Dependencies {
			errs = append(errs, abort(dependency))
		}
		if p.Abort != nil {
			errs = append(errs, p.Abort())
		}
		if source, ok := p.Source.(CloseableSource); ok {
			errs = append(errs, source.Close())
		}
		errs = append(errs, closeSink(p.Sink))
		return errors.Join(errs...)
	}
	return abort(root)
}
