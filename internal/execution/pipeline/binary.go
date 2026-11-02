package pipeline

// BinaryBuildSide 表示双输入 Push operator 先 materialize 哪一侧。
type BinaryBuildSide uint8

const (
	// BinaryBuildLeft 表示左 child 是 build dependency，右 child 是 probe pipeline。
	BinaryBuildLeft BinaryBuildSide = iota
	// BinaryBuildRight 表示右 child 是 build dependency，左 child 是 probe pipeline。
	BinaryBuildRight
)

// BinaryOperator 是 join 一类 build/probe 双输入算子的 generic Pipeline contract。
// BuildSink 必须先成功 Finalize，ProbeSink 才能消费；Close 必须幂等并释放全部 query-scoped ownership。
type BinaryOperator interface {
	BuildSide() BinaryBuildSide
	BuildSink() Sink
	ProbeSink(Sink) Sink
	Close()
}
