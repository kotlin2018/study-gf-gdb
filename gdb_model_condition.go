// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import (
	"strings"
)

// Where 设置模型的条件语句。参数<where>可以是string/map/gmap/slice/struct/*struct等类型。
// 请注意，如果多次调用它，将使用“AND”将多个条件连接到where语句中。
//
// Eg:
//
// Where("uid=10000") 等价于 Where("uid", 10000)
//
// Where("money>? AND name like ?", 99999, "vip_%")
//
// Where("uid", 1).Where("name", "john")
//
// Where("status IN (?)", g.Slice{1,2,3})
//
// Where("age IN(?,?)", 18, 50)
//
// Where(User{ Id : 1, UserName : "john"})
//
// Where("uid <=?", 1000).Where("age >=?", 18)等价于 where (`uid` <= 1000) and (`age` >= 18)；
//
// Where("level=? OR money >=?", 1, 1000000)等价于 where `level`=1 or `money`>=1000000
func (m *Model) Where(where interface{}, args ...interface{}) *Model {
	model := m.getModel()
	if model.whereHolder == nil {
		model.whereHolder = make([]*whereHolder, 0)
	}
	model.whereHolder = append(model.whereHolder, &whereHolder{
		operator: whereHolderWhere,
		where:    where,
		args:     args,
	})
	return model
}

// Having 设置模型的having语句。
// 此函数用法的参数与函数Where相同。
func (m *Model) Having(having interface{}, args ...interface{}) *Model {
	model := m.getModel()
	model.having = []interface{}{
		having, args,
	}
	return model
}

// WherePri方法的功能同Where，但提供了对表主键的智能识别。
// 如果主键是“id”，并且给定<where>参数为“123”，则WherePri函数将条件视为“id=123”，而M.where将条件视为字符串“123”。
func (m *Model) WherePri(where interface{}, args ...interface{}) *Model {
	if len(args) > 0 {
		return m.Where(where, args...)
	}
	newWhere := GetPrimaryKeyCondition(m.getPrimaryKey(), where)
	return m.Where(newWhere[0], newWhere[1:]...)
}

// And 在where语句中添加“AND”条件。
func (m *Model) And(where interface{}, args ...interface{}) *Model {
	model := m.getModel()
	if model.whereHolder == nil {
		model.whereHolder = make([]*whereHolder, 0)
	}
	model.whereHolder = append(model.whereHolder, &whereHolder{
		operator: whereHolderAnd,
		where:    where,
		args:     args,
	})
	return model
}

// Or 在where语句中添加“OR”条件。
func (m *Model) Or(where interface{}, args ...interface{}) *Model {
	model := m.getModel()
	if model.whereHolder == nil {
		model.whereHolder = make([]*whereHolder, 0)
	}
	model.whereHolder = append(model.whereHolder, &whereHolder{
		operator: whereHolderOr,
		where:    where,
		args:     args,
	})
	return model
}

// Group 分组 (设置模型的“group by”语句)。
func (m *Model) Group(groupBy string) *Model {
	model := m.getModel()
	model.groupBy = m.db.QuoteString(groupBy)
	return model
}

// GroupBy Model.Group的别名
func (m *Model) GroupBy(groupBy string) *Model {
	return m.Group(groupBy)
}

// Order 排序 (设置模型的“order by”语句)。
func (m *Model) Order(orderBy ...string) *Model {
	model := m.getModel()
	model.orderBy = m.db.QuoteString(strings.Join(orderBy, " "))
	return model
}

// OrderBy Model.Order的别名
func (m *Model) OrderBy(orderBy string) *Model {
	return m.Order(orderBy)
}

// Limit 设置模型的“limit”语句。
//
// 参数<limit>可以是一个或两个数字，如果传递了两个数字，则为模型设置“limit limit[0]、limit[1]”语句，否则设置“limit limit[0]”语句。
func (m *Model) Limit(limit ...int) *Model {
	model := m.getModel()
	switch len(limit) {
	case 1:
		model.limit = limit[0]
	case 2:
		model.start = limit[0]
		model.limit = limit[1]
	}
	return model
}

// Offset 设置模型的“offset”语句。
//
// 它只适用于某些数据库，如SQLServer、PostgreSQL等。
func (m *Model) Offset(offset int) *Model {
	model := m.getModel()
	model.offset = offset
	return model
}

// Page 设置模型的页码。参数<page>从1开始分页。
//
// 注意，对于“Limit”语句，Limit函数从0开始是不同的。
func (m *Model) Page(page, limit int) *Model {
	model := m.getModel()
	if page <= 0 {
		page = 1
	}
	model.start = (page - 1) * limit
	model.limit = limit
	return model
}

// ForPage Model.Page的别名
func (m *Model) ForPage(page, limit int) *Model {
	return m.Page(page, limit)
}
