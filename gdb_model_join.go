// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import (
	"fmt"
	"github.com/gogf/gf/text/gstr"
)

// isSubQuery 检查并返回给定字符串是否为子查询sql字符串。
func isSubQuery(s string) bool {
	s = gstr.TrimLeft(s, "()")
	if p := gstr.Pos(s, " "); p != -1 {
		if gstr.Equal(s[:p], "select") {
			return true
		}
	}
	return false
}

// LeftJoin 对Model执行“left join ... on ...”语句。
//
// 参数<table>可以是联接表及其联接条件，也可以是其别名，例如:
//
// Table("user").LeftJoin("user_detail", "user_detail.uid=user.uid");
//
// Table("user", "u").LeftJoin("user_detail", "ud", "ud.uid=u.uid");
//
// Table("user", "u").LeftJoin("SELECT xxx FROM xxx AS a", "a.uid=u.uid")
func (m *Model) LeftJoin(table ...string) *Model {
	return m.doJoin("LEFT", table...)
}

// RightJoin 对Model执行“right join ... on ...”语句。
//
// 参数<table>可以是联接表及其联接条件，也可以是其别名，例如:
//
// Table("user").RightJoin("user_detail", "user_detail.uid=user.uid");
//
// Table("user", "u").RightJoin("user_detail", "ud", "ud.uid=u.uid");
//
// Table("user", "u").RightJoin("SELECT xxx FROM xxx AS a", "a.uid=u.uid")
func (m *Model) RightJoin(table ...string) *Model {
	return m.doJoin("RIGHT", table...)
}

// InnerJoin 对模型执行“inner join ... on ...”语句。
//
// 参数<table>可以是联接表及其联接条件，也可以是其别名，例如:
//
// Table("user").InnerJoin("user_detail", "user_detail.uid=user.uid");
//
// Table("user", "u").InnerJoin("user_detail", "ud", "ud.uid=u.uid");
//
// Table("user", "u").InnerJoin("SELECT xxx FROM xxx AS a", "a.uid=u.uid")
func (m *Model) InnerJoin(table ...string) *Model {
	return m.doJoin("INNER", table...)
}

// doJoin 对模型执行 "left/right/inner join ... on ..." 语句。
//
// 参数<table>可以是联接表及其联接条件，也可以是其别名，如：
//
// Table("user").InnerJoin("user_detail", "user_detail.uid=user.uid");
//
// Table("user", "u").InnerJoin("user_detail", "ud", "ud.uid=u.uid");
//
// Table("user", "u").InnerJoin("SELECT xxx FROM xxx AS a", "a.uid=u.uid")//
//
// 相关问题: https://github.com/gogf/gf/issues/1024
func (m *Model) doJoin(operator string, table ...string) *Model {
	var (
		model   = m.getModel()
		joinStr = ""
	)
	if len(table) > 0 {
		if isSubQuery(table[0]) {
			joinStr = gstr.Trim(table[0])
			if joinStr[0] != '(' {
				joinStr = "(" + joinStr + ")"
			}
		} else {
			joinStr = m.db.QuotePrefixTableName(table[0])
		}
	}
	if len(table) > 2 {
		model.tables += fmt.Sprintf(
			" %s JOIN %s AS %s ON (%s)",
			operator, joinStr, m.db.QuoteWord(table[1]), table[2],
		)
	} else if len(table) == 2 {
		model.tables += fmt.Sprintf(
			" %s JOIN %s ON (%s)",
			operator, joinStr, table[1],
		)
	} else if len(table) == 1 {
		model.tables += fmt.Sprintf(
			" %s JOIN %s", operator, joinStr,
		)
	}
	return model
}
