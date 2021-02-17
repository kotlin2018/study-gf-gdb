// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import (
	"context"
	"database/sql"
	"github.com/gogf/gf/errors/gerror"
	"github.com/gogf/gf/os/gtime"
)

// Stmt is a prepared statement。Stmt对于多个goroutine并发使用是安全的。
//
// 如果Stmt是在Tx或Conn上准备的，那么它将永远绑定到单个底层连接；
//
// 如果Tx或Conn关闭，Stmt将不可用，所有操作将返回错误；
//
// 如果在DB上准备了Stmt，它将在DB的生命周期内保持可用；
//
// 当Stmt需要在新的基础连接上执行时，它将自动在新连接上准备自己。
type Stmt struct {
	*sql.Stmt
	core *Core
	sql  string
}

const (
	stmtTypeExecContext     = "Statement.ExecContext"
	stmtTypeQueryContext    = "Statement.QueryContext"
	stmtTypeQueryRowContext = "Statement.QueryRowContext"
)

// doStmtCommit 根据给定的“stmtType”提交语句。
func (s *Stmt) doStmtCommit(stmtType string, ctx context.Context, args ...interface{}) (result interface{}, err error) {
	var (
		cancelFuncForTimeout context.CancelFunc
		timestampMilli1      = gtime.TimestampMilli()
	)
	switch stmtType {
	case stmtTypeExecContext:
		ctx, cancelFuncForTimeout = s.core.GetCtxTimeout(ctxTimeoutTypeExec, ctx)
		defer cancelFuncForTimeout()
		result, err = s.Stmt.ExecContext(ctx, args...)

	case stmtTypeQueryContext:
		ctx, cancelFuncForTimeout = s.core.GetCtxTimeout(ctxTimeoutTypeQuery, ctx)
		defer cancelFuncForTimeout()
		result, err = s.Stmt.QueryContext(ctx, args...)

	case stmtTypeQueryRowContext:
		ctx, cancelFuncForTimeout = s.core.GetCtxTimeout(ctxTimeoutTypeQuery, ctx)
		defer cancelFuncForTimeout()
		result = s.Stmt.QueryRowContext(ctx, args...)

	default:
		panic(gerror.Newf(`invalid stmtType: %s`, stmtType))
	}
	var (
		timestampMilli2 = gtime.TimestampMilli()
		sqlObj          = &Sql{
			Sql:    s.sql,
			Type:   stmtType,
			Args:   args,
			Format: FormatSqlWithArgs(s.sql, args),
			Error:  err,
			Start:  timestampMilli1,
			End:    timestampMilli2,
			Group:  s.core.DB.GetGroup(),
		}
	)
	s.core.addSqlToTracing(ctx, sqlObj)
	if s.core.DB.GetDebug() {
		s.core.writeSqlToLogger(sqlObj)
	}
	return result, err
}

// ExecContext 用给定的参数执行一个准备好的语句，并返回一个总结语句效果的结果。
func (s *Stmt) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	result, err := s.doStmtCommit(stmtTypeExecContext, ctx, args...)
	if result != nil {
		return result.(sql.Result), err
	}
	return nil, err
}

// QueryContext 使用给定的参数执行准备好的查询语句，并以*行的形式返回查询结果。
func (s *Stmt) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {
	result, err := s.doStmtCommit(stmtTypeQueryContext, ctx, args...)
	if result != nil {
		return result.(*sql.Rows), err
	}
	return nil, err
}

// QueryRowContext 使用给定的参数执行准备好的查询语句。
//
// 如果在语句执行期间发生错误，则调用Scan返回的*行将返回该错误，该行始终为非nil。
//
// 如果查询没有选择行，*Row's的扫描将返回errnorow。否则，*Row's将扫描第一个选定行并丢弃其余行。
func (s *Stmt) QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row {
	result, _ := s.doStmtCommit(stmtTypeQueryRowContext, ctx, args...)
	if result != nil {
		return result.(*sql.Row)
	}
	return nil
}

// Exec 用给定的参数执行一个准备好的语句，并返回一个总结语句效果的结果。
func (s *Stmt) Exec(args ...interface{}) (sql.Result, error) {
	return s.ExecContext(context.Background(), args...)
}

// Query 使用给定的参数执行准备好的查询语句，并以*Rows的形式返回查询结果。
func (s *Stmt) Query(args ...interface{}) (*sql.Rows, error) {
	return s.QueryContext(context.Background(), args...)
}

// QueryRow 使用给定的参数执行准备好的查询语句。
//
// 如果在语句执行期间发生错误，则调用Scan返回的*Row将返回该错误，该行始终为非nil。ErrNoRows.
//
// 如果查询没有选择行，*行的扫描将返回ErrNoRows。否则，*Row将扫描第一个选定行并丢弃其余行。
//
// 示例用法:
//
// var name string err := nameByUseridStmt.QueryRow(id).Scan(&name)
func (s *Stmt) QueryRow(args ...interface{}) *sql.Row {
	return s.QueryRowContext(context.Background(), args...)
}

// Close 关闭语句。
func (s *Stmt) Close() error {
	return s.Stmt.Close()
}
