package session

import (
	"context"
	"fmt"
	"strings"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/security"
	"github.com/steamwo/fssql/internal/sql/binder"
)

const maxSecretTypes = 64

// SQLControlAdapter 执行不需要 query data pipeline 的 Stable control statement。
// SELECT/COPY/DESCRIBE 不属于此路径，避免绕过统一 Query/Export 执行边界。
type SQLControlAdapter struct {
	secrets     *security.SecretManager // secrets 是 CREATE SECRET 唯一允许写入的 credential store。
	secretTypes map[string]struct{}      // secretTypes 是冻结的小型 TYPE adapter allowlist；TYPE 不写入 credential map。
}

// NewSQLControlAdapter 创建 control adapter，并冻结 SQL CREATE SECRET 可用的 TYPE metadata。
// nil SecretManager 允许只执行 Catalog DDL；CREATE SECRET 会显式失败。
func NewSQLControlAdapter(secrets *security.SecretManager, secretTypes []string) (*SQLControlAdapter, error) {
	if len(secretTypes) > maxSecretTypes {
		return nil, fmt.Errorf("session: too many secret types: %d > %d", len(secretTypes), maxSecretTypes)
	}
	types := make(map[string]struct{}, len(secretTypes))
	for _, value := range secretTypes {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			return nil, fmt.Errorf("session: secret type must not be empty")
		}
		if _, exists := types[value]; exists {
			return nil, fmt.Errorf("session: duplicate secret type %s", value)
		}
		types[value] = struct{}{}
	}
	return &SQLControlAdapter{secrets: secrets, secretTypes: types}, nil
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
		if a == nil || a.secrets == nil {
			return fmt.Errorf("session: SecretManager is not configured")
		}
		if !a.supportsSecretType(value.Type) {
			return fmt.Errorf("session: unsupported secret TYPE %s", value.Type)
		}
		return applyCreateSecret(a.secrets, value)
	default:
		return fmt.Errorf("session: statement kind %d is not a control-statement adapter responsibility", bound.StatementKind())
	}
}

// supportsSecretType 验证 TYPE adapter metadata；TYPE 只参与 adapter selection，不进入 credential storage。
func (a *SQLControlAdapter) supportsSecretType(value string) bool {
	if a == nil {
		return false
	}
	_, ok := a.secretTypes[strings.ToLower(strings.TrimSpace(value))]
	return ok
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
	var createErr error
	if value.Temporary {
		if value.Replace {r,ok:=tx.(interface{ReplaceTemporaryView(context.Context,catalog.View)error});if !ok{return fmt.Errorf("session catalog does not support temporary view replacement")};createErr=r.ReplaceTemporaryView(ctx,view)}else{r,ok:=tx.(interface{CreateTemporaryView(context.Context,catalog.View)error});if !ok{return fmt.Errorf("session catalog does not support temporary views")};createErr=r.CreateTemporaryView(ctx,view)}
	} else if value.Replace {
		replacer, ok := tx.(interface{ ReplaceView(context.Context, catalog.View) error })
		if !ok { return fmt.Errorf("session: catalog does not support atomic view replacement") }
		createErr = replacer.ReplaceView(ctx,view)
	} else { createErr = tx.CreateView(ctx,view) }
	if createErr != nil {
		return createErr
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

// applyCreateSecret 将 bound credential 明文只复制到 SecretManager；TYPE 作为独立 metadata 保存，不进入 Values。
func applyCreateSecret(manager *security.SecretManager, value *binder.BoundCreateSecret) error {
	if value == nil {
		return fmt.Errorf("session: invalid bound CREATE SECRET")
	}
	credentials := make(map[string]string, len(value.Credentials))
	for _, credential := range value.Credentials {
		credentials[credential.Key] = credential.Value()
	}
	return manager.Create(security.SecretSpec{
		Name:   value.Name,
		Type:   value.Type,
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
