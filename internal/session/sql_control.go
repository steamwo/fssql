package session

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/security"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// SQLControlAdapter 执行不需要 query data pipeline 的 Stable control statement。
// SELECT/COPY/DESCRIBE 不属于此路径，避免绕过统一 Query/Export 执行边界。
type SQLControlAdapter struct {
	Secrets *security.SecretManager // Secrets 是 CREATE SECRET 唯一允许写入的 credential store。
}

// Apply 执行已绑定 control statement；Binder 已负责语法与语义校验，这里只做 subsystem 映射和 ownership commit。
func (a *SQLControlAdapter) Apply(ctx context.Context, statement *Statement, bound binder.BoundStatement) error {
	if statement == nil || bound == nil {
		return fmt.Errorf("session: control statement requires statement and bound input")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	switch value := bound.(type) {
	case *binder.BoundCreateView:
		return applyCreateView(ctx, statement, value)
	case *binder.BoundCreateExternalTable:
		return applyCreateExternalTable(ctx, statement, value)
	case *binder.BoundCreateSecret:
		if a == nil || a.Secrets == nil {
			return fmt.Errorf("session: SecretManager is not configured")
		}
		return applyCreateSecret(a.Secrets, value)
	default:
		return fmt.Errorf("session: statement kind %d is not a control-statement adapter responsibility", bound.StatementKind())
	}
}

// applyCreateView 写当前 Statement 独占 Catalog Transaction，并仅在 metadata 构造成功后提交。
func applyCreateView(ctx context.Context, statement *Statement, value *binder.BoundCreateView) error {
	if value == nil || value.Query == nil || len(value.Columns) == 0 {
		return fmt.Errorf("session: invalid bound CREATE VIEW")
	}
	tx := statement.Catalog()
	if tx == nil {
		return fmt.Errorf("session: statement catalog transaction is unavailable")
	}
	view := catalog.View{
		Name:    catalog.ObjectName{Schema: value.Name.Schema, Name: value.Name.Name},
		SQL:     value.SQL,
		Columns: catalogColumns(value.Columns),
	}
	if err := tx.CreateView(ctx, view); err != nil {
		return err
	}
	return statement.CommitCatalog(ctx)
}

// applyCreateExternalTable 持久化 Binder 的真实 TableFunction.Bind schema；credential 永不进入 Catalog payload。
func applyCreateExternalTable(ctx context.Context, statement *Statement, value *binder.BoundCreateExternalTable) error {
	if value == nil || value.Function == "" || value.Source == "" || len(value.Columns) == 0 {
		return fmt.Errorf("session: invalid bound CREATE EXTERNAL TABLE")
	}
	tx := statement.Catalog()
	if tx == nil {
		return fmt.Errorf("session: statement catalog transaction is unavailable")
	}
	table := catalog.ExternalTable{
		Name:          catalog.ObjectName{Schema: value.Name.Schema, Name: value.Name.Name},
		Columns:       catalogColumns(value.Columns),
		TableFunction: value.Function,
		Source:        value.Source,
	}
	if err := tx.CreateExternalTable(ctx, table); err != nil {
		return err
	}
	return statement.CommitCatalog(ctx)
}

// applyCreateSecret 将 bound credential 明文只复制到 SecretManager；错误与审计路径不格式化 credential value。
func applyCreateSecret(manager *security.SecretManager, value *binder.BoundCreateSecret) error {
	if value == nil {
		return fmt.Errorf("session: invalid bound CREATE SECRET")
	}
	credentials := make(map[string]string, len(value.Credentials)+1)
	// TYPE 是 adapter metadata；SecretManager 当前只有 key/value credential store，因此使用保留键保存类型。
	// 该值不是 credential secret，但与 credential 一起只存在 SecretManager，不写入 Catalog。
	credentials["type"] = value.Type
	for _, credential := range value.Credentials {
		credentials[credential.Key] = credential.Value()
	}
	return manager.Create(security.SecretSpec{
		Name:   value.Name,
		Values: credentials,
		Scope: security.SecretScope{
			Subjects: append([]string(nil), value.Subjects...),
			Schemes:  append([]string(nil), value.Schemes...),
			Hosts:    append([]string(nil), value.Hosts...),
		},
	})
}

// catalogColumns 把 Binder relation schema 映射为 Catalog metadata，不创建 runtime Vector/reader。
func catalogColumns(columns []binder.RelationColumn) []catalog.ColumnDefinition {
	out := make([]catalog.ColumnDefinition, len(columns))
	for i, column := range columns {
		out[i] = catalog.ColumnDefinition{Name: column.Name, Type: column.Type, Nullable: column.Nullable}
	}
	return out
}
