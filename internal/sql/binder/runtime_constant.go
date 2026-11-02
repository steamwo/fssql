package binder

// BoundRuntimeConstant 是 Stable non-correlated scalar subquery 的执行期单值占位。
// 它追加在既有 ExpressionKind 后，不改变已有 kind 数值；Literal 保存十进制 runtime constant ID。
const BoundRuntimeConstant ExpressionKind = BoundSubquery + 1
