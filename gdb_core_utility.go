// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.
//

package gdb

import (
	"database/sql"
)

// GetMaster 作用类似于函数主控，但带有指定连接模式的附加<schema>参数，它是为内部用法。还有见 Master.
func (c *Core) GetMaster(schema ...string) (*sql.DB, error) {
	return c.getSqlDb(true, schema...)
}

// GetSlave acts like function Slave but with additional <schema> parameter specifying
// the schema for the connection. It is defined for internal usage.
// Also see Slave.
func (c *Core) GetSlave(schema ...string) (*sql.DB, error) {
	return c.getSqlDb(false, schema...)
}

// QuoteWord checks given string <s> a word, if true quotes it with security chars of the database
// and returns the quoted string; or else return <s> without any change.
func (c *Core) QuoteWord(s string) string {
	charLeft, charRight := c.DB.GetChars()
	return doQuoteWord(s, charLeft, charRight)
}

// QuoteString quotes string with quote chars. Strings like:
// "user", "user u", "user,user_detail", "user u, user_detail ut", "u.id asc".
func (c *Core) QuoteString(s string) string {
	charLeft, charRight := c.DB.GetChars()
	return doQuoteString(s, charLeft, charRight)
}

// QuotePrefixTableName adds prefix string and quotes chars for the table.
// It handles table string like:
// "user", "user u",
// "user,user_detail",
// "user u, user_detail ut",
// "user as u, user_detail as ut".
//
// Note that, this will automatically checks the table prefix whether already added,
// if true it does nothing to the table name, or else adds the prefix to the table name.
func (c *Core) QuotePrefixTableName(table string) string {
	charLeft, charRight := c.DB.GetChars()
	return doHandleTableName(table, c.DB.GetPrefix(), charLeft, charRight)
}

// GetChars returns the security char for current database.
// It does nothing in default.
func (c *Core) GetChars() (charLeft string, charRight string) {
	return "", ""
}

// HandleSqlBeforeCommit 在将sql发布到数据库之前处理它，它在默认情况下什么也不做。
func (c *Core) HandleSqlBeforeCommit(sql string) string {
	return sql
}

// Tables 检索并返回当前架构的表，它主要用于cli工具链中自动生成模型。它默认情况下不执行任何操作。
func (c *Core) Tables(schema ...string) (tables []string, err error) {
	return
}

// TableFields 检索并返回当前架构的指定表的字段信息。请注意，它返回一个包含字段名及其对应字段的映射。
//
// 当map未排序时，TableField结构会有一个“Index”字段来标记其在字段中的序列。
//
// 它使用缓存特性来提高性能，这在进程重新启动时是永远不会过期的，它在默认情况下什么也不做。
func (c *Core) TableFields(table string, schema ...string) (fields map[string]*TableField, err error) {
	return
}
