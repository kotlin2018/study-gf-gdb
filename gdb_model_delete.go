// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import (
	"database/sql"
	"fmt"
	"github.com/gogf/gf/errors/gerror"
	"github.com/gogf/gf/os/gtime"
	"github.com/gogf/gf/text/gstr"
)

// Delete 对表执行 "delete from ... "语句。
//
// 用于数据的永久删除，被删除的数据不可恢复，请慎重使用。
//
// 可选参数<where>与Model.Where()的参数相同
func (m *Model) Delete(where ...interface{}) (result sql.Result, err error) {
	if len(where) > 0 {
		return m.Where(where[0], where[1:]...).Delete()
	}
	defer func() {
		if err == nil {
			m.checkAndRemoveCache()
		}
	}()
	var (
		fieldNameDelete                               = m.getSoftFieldNameDeleted()
		conditionWhere, conditionExtra, conditionArgs = m.formatCondition(false, false)
	)
	// Soft deleting.
	if !m.unscoped && fieldNameDelete != "" {
		return m.db.DoUpdate(
			m.getLink(true),
			m.tables,
			fmt.Sprintf(`%s=?`, m.db.QuoteString(fieldNameDelete)),
			conditionWhere+conditionExtra,
			append([]interface{}{gtime.Now().String()}, conditionArgs...),
		)
	}
	conditionStr := conditionWhere + conditionExtra
	if !gstr.ContainsI(conditionStr, " WHERE ") {
		return nil, gerror.New("there should be WHERE condition statement for DELETE operation")
	}
	return m.db.DoDelete(m.getLink(true), m.tables, conditionStr, conditionArgs...)
}
