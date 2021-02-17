// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

// Schema 是一个模式对象，然后可以从中创建模型。
type Schema struct {
	db     DB
	tx     *TX
	schema string
}

// Schema 创建并返回架构。
func (c *Core) Schema(schema string) *Schema {
	return &Schema{
		db:     c.DB,
		schema: schema,
	}
}

// Schema 从模式中创建并返回初始化模型，然后可以从中创建模型。
func (tx *TX) Schema(schema string) *Schema {
	return &Schema{
		tx:     tx,
		schema: schema,
	}
}

// Table 创建并返回新的ORM Model。参数<tables>可以是多个表名。如:
//
// “user”，“user u”，“user，user\u detail”，“user u，user\u detail ud”
func (s *Schema) Table(table string) *Model {
	var m *Model
	if s.tx != nil {
		m = s.tx.Table(table)
	} else {
		m = s.db.Table(table)
	}
	// 不要更改原始数据库的模式，它在这里创建一个新的数据库并更改其模式。
	db, err := New(m.db.GetGroup())
	if err != nil {
		panic(err)
	}
	db.SetSchema(s.schema)
	m.db = db
	m.schema = s.schema
	return m
}

// Model  Core.Table的别名。
func (s *Schema) Model(table string) *Model {
	return s.Table(table)
}
