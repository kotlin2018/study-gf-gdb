// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/gogf/gf/text/gregex"
)

// TX 是事务管理的结构体。
type TX struct {
	db     DB
	tx     *sql.Tx
	master *sql.DB
}

// Commit 提交事务
func (tx *TX) Commit() error {
	return tx.tx.Commit()
}

// Rollback 中止事务(事务回滚)
func (tx *TX) Rollback() error {
	return tx.tx.Rollback()
}

// Query 对事务执行查询操作
func (tx *TX) Query(sql string, args ...interface{}) (rows *sql.Rows, err error) {
	return tx.db.DoQuery(tx.tx, sql, args...)
}

// Exec 对事务执行: 增加/删除/更新操作 (不包括查询操作)
func (tx *TX) Exec(sql string, args ...interface{}) (sql.Result, error) {
	return tx.db.DoExec(tx.tx, sql, args...)
}

// Prepare 预加载 (为以后的查询或执行创建准备好的语句)
//
// 可以从返回的语句同时运行多个查询(Query)或执行(Exec)，当不再需要该语句时，调用方必须调用该语句的Close方法。
func (tx *TX) Prepare(sql string) (*Stmt, error) {
	return tx.db.DoPrepare(tx.tx, sql)
}

// All 查询并返回数据库中的数据记录。
func (tx *TX) All(sql string, args ...interface{}) (Result, error) {
	rows, err := tx.Query(sql, args...)
	if err != nil || rows == nil {
		return nil, err
	}
	defer rows.Close()
	return tx.db.convertRowsToResult(rows)
}

// One 查询并从数据库返回一条记录。
func (tx *TX) One(sql string, args ...interface{}) (Record, error) {
	list, err := tx.All(sql, args...)
	if err != nil {
		return nil, err
	}
	if len(list) > 0 {
		return list[0], nil
	}
	return nil, nil
}

// Struct 从数据库中查询一条记录并将其转换为给定的结构，参数<pointer>应该是指向struct的指针。
func (tx *TX) Struct(obj interface{}, sql string, args ...interface{}) error {
	one, err := tx.One(sql, args...)
	if err != nil {
		return err
	}
	return one.Struct(obj)
}

// Structs 查询数据库中的记录并将其转换为给定的结构，参数<pointer>的类型应为struct slice:[]struct/[]*struct。
func (tx *TX) Structs(objPointerSlice interface{}, sql string, args ...interface{}) error {
	all, err := tx.All(sql, args...)
	if err != nil {
		return err
	}
	return all.Structs(objPointerSlice)
}

// Scan 查询数据库中的一个或多个记录，并将它们转换为给定的结构体或结构体数组。
//
// 如果参数<pointer>是struct pointer的类型，它将在内部调用Struct进行转换。
//
// 如果参数<pointer>是片的类型，它将在内部调用Structs进行转换。
func (tx *TX) Scan(objPointer interface{}, sql string, args ...interface{}) error {
	t := reflect.TypeOf(objPointer)
	k := t.Kind()
	if k != reflect.Ptr {
		return fmt.Errorf("params should be type of pointer, but got: %v", k)
	}
	k = t.Elem().Kind()
	switch k {
	case reflect.Array, reflect.Slice:
		return tx.db.GetStructs(objPointer, sql, args...)
	case reflect.Struct:
		return tx.db.GetStruct(objPointer, sql, args...)
	default:
		return fmt.Errorf("element type should be type of struct/slice, unsupported: %v", k)
	}
}

// Value 用于查询并返回数据库中的一个字段值，往往需要结合Fields方法使用。
//
// ql应该只从数据库中查询一个字段，否则它只返回结果的一个字段。
func (tx *TX) Value(sql string, args ...interface{}) (Value, error) {
	one, err := tx.One(sql, args...)
	if err != nil {
		return nil, err
	}
	for _, v := range one {
		return v, nil
	}
	return nil, nil
}

// Count 用于查询并返回数据库中表的记录数。
func (tx *TX) Count(sql string, args ...interface{}) (int, error) {
	if !gregex.IsMatchString(`(?i)SELECT\s+COUNT\(.+\)\s+FROM`, sql) {
		sql, _ = gregex.ReplaceString(`(?i)(SELECT)\s+(.+)\s+(FROM)`, `$1 COUNT($2) $3`, sql)
	}
	value, err := tx.Value(sql, args...)
	if err != nil {
		return 0, err
	}
	return value.Int(), nil
}

// Insert 对表执行 "insert into ..." 语句。如果表中已经有一个唯一的数据记录，它将返回error。
//
// 参数<data>可以是map/gmap/struct/*struct/[]map/[]struct等类型。
//
// 例如:
//
// Data(g.Map{"uid": 10000, "name":"john"})
//
// Data(g.Slice{g.Map{"uid": 10000, "name":"john"}, g.Map{"uid": 20000, "name":"smith"})
//
// 参数<batch>指定给定数据为切片时的批处理操作计数。例如: batch = 10表示一次批处理10条记录。
func (tx *TX) Insert(table string, data interface{}, batch ...int) (sql.Result, error) {
	if len(batch) > 0 {
		return tx.Model(table).Data(data).Batch(batch[0]).Insert()
	}
	return tx.Model(table).Data(data).Insert()
}

// InsertIgnore 对表执行 "insert ignore into ..." 语句。如果表中已经有一个唯一的数据记录，则忽略插入。
//
// 参数<data>可以是map/gmap/struct/*struct/[]map/[]struct等类型。
//
// 例如:
//
// Data(g.Map{"uid": 10000, "name":"john"})
//
// Data(g.Slice{g.Map{"uid": 10000, "name":"john"}, g.Map{"uid": 20000, "name":"smith"})
//
// 参数<batch>指定给定数据为切片时的批处理操作计数。例如: batch = 10 表示一次批处理10条记录。
func (tx *TX) InsertIgnore(table string, data interface{}, batch ...int) (sql.Result, error) {
	if len(batch) > 0 {
		return tx.Model(table).Data(data).Batch(batch[0]).InsertIgnore()
	}
	return tx.Model(table).Data(data).InsertIgnore()
}

// Replace 对表执行 "replace into ..." 语句。如果表中已经有一条数据的唯一记录，它将删除该记录并插入一条新记录。
//
// 参数<data>可以是map/gmap/struct/*struct/[]map/[]struct等类型。
//
// 例如:
//
// Data(g.Map{"uid": 10000, "name":"john"})
//
// Data(g.Slice{g.Map{"uid": 10000, "name":"john"}, g.Map{"uid": 20000, "name":"smith"})
//
// 如果给定的数据是切片类型，那么它将执行批替换，可选参数<batch>指定批操作计数。
func (tx *TX) Replace(table string, data interface{}, batch ...int) (sql.Result, error) {
	if len(batch) > 0 {
		return tx.Model(table).Data(data).Batch(batch[0]).Replace()
	}
	return tx.Model(table).Data(data).Replace()
}

// Save 对表执行 "insert into ... on duplicate key update..." 语句。
// 如果保存的数据中有主索引或唯一索引，它会更新记录，否则会将新记录插入表中。
//
// 参数<data>可以是map/gmap/struct/*struct/[]map/[]struct等类型。
//
// 例如:
//
// Data(g.Map{"uid": 10000, "name":"john"})
//
// Data(g.Slice{g.Map{"uid": 10000, "name":"john"}, g.Map{"uid": 20000, "name":"smith"})
//
// 如果给定的数据是切片类型，则执行批处理保存，可选参数<batch>指定批处理操作计数。
func (tx *TX) Save(table string, data interface{}, batch ...int) (sql.Result, error) {
	if len(batch) > 0 {
		return tx.Model(table).Data(data).Batch(batch[0]).Save()
	}
	return tx.Model(table).Data(data).Save()
}

// BatchInsert 批量插入数据。
// 参数<list>必须是map或struct的切片类型。
func (tx *TX) BatchInsert(table string, list interface{}, batch ...int) (sql.Result, error) {
	if len(batch) > 0 {
		return tx.Model(table).Data(list).Batch(batch[0]).Insert()
	}
	return tx.Model(table).Data(list).Insert()
}

// BatchInsertIgnore 批插入带有忽略选项的数据。
// 参数<list>必须是map或struct的切片类型。
func (tx *TX) BatchInsertIgnore(table string, list interface{}, batch ...int) (sql.Result, error) {
	if len(batch) > 0 {
		return tx.Model(table).Data(list).Batch(batch[0]).InsertIgnore()
	}
	return tx.Model(table).Data(list).InsertIgnore()
}

// BatchReplace 批量替换数据。
// 参数<list>必须是map或struct的切片类型。
func (tx *TX) BatchReplace(table string, list interface{}, batch ...int) (sql.Result, error) {
	if len(batch) > 0 {
		return tx.Model(table).Data(list).Batch(batch[0]).Replace()
	}
	return tx.Model(table).Data(list).Replace()
}

// BatchSave 批量替换数据。
// 参数<list>必须是map或struct的切片类型。
func (tx *TX) BatchSave(table string, list interface{}, batch ...int) (sql.Result, error) {
	if len(batch) > 0 {
		return tx.Model(table).Data(list).Batch(batch[0]).Save()
	}
	return tx.Model(table).Data(list).Save()
}

// Update 对表执行 "update ... " 语句。
//
// 参数<data>可以是string/map/gmap/struct/*struct等类型。
//
// 参数<condition>可以是string/map/gmap/slice/struct/*struct等类型，它通常与参数<args>一起使用。
//
// 例如:
//
// "uid=10000", "uid", 10000, g.Map{"uid": 10000, "name":"john"}
//
// "uid=10000" 等价于 "uid", 10000
//
// "money>? AND name like ?", 99999, "vip_%"
//
// "status IN (?)", g.Slice{1,2,3}
//
// "age IN(?,?)", 18, 50
//
// User{ Id : 1, UserName : "john"}
func (tx *TX) Update(table string, data interface{}, condition interface{}, args ...interface{}) (sql.Result, error) {
	return tx.Model(table).Data(data).Where(condition, args...).Update()
}

// Delete 对表执行 "delete from ..." 语句。
//
// 参数<condition>的类型可以是string/map/gmap/slice/struct/*struct等等，它通常与参数<args>一起使用。
//
// 例如:
//
// "uid=10000" 等价于 "uid", 10000
//
// "money>? AND name like ?", 99999, "vip_%"
//
// "status IN (?)", g.Slice{1,2,3}
//
// "age IN(?,?)", 18, 50
//
// User{ Id : 1, UserName : "john"}
func (tx *TX) Delete(table string, condition interface{}, args ...interface{}) (sql.Result, error) {
	return tx.Model(table).Where(condition, args...).Delete()
}
