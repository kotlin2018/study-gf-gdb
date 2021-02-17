// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import (
	"database/sql"
	"github.com/gogf/gf/errors/gerror"
	"github.com/gogf/gf/os/gtime"
	"github.com/gogf/gf/text/gstr"
	"github.com/gogf/gf/util/gconv"
	"github.com/gogf/gf/util/gutil"
	"reflect"
)

// Batch 设置model的批处理操作数量。
func (m *Model) Batch(batch int) *Model {
	model := m.getModel()
	model.batch = batch
	return model
}

// Data 设置model的操作数据，参数<data>可以是string/map/gmap/slice/struct/*struct等类型。
//
// 请注意: 如果“data”是map/slice类型，则它对“data”使用浅值复制，以避免在函数内更改它。
//
// Eg:
//
// Data("uid=10000") 等价于 Data("uid", 10000);
//
// Data("uid=? AND name=?", 10000, "john") 等价于 Data(g.Map{"uid": 10000, "name":"john"});
//
// Data(g.Slice{g.Map{"uid": 10000, "name":"john"}, g.Map{"uid": 20000, "name":"smith"})
func (m *Model) Data(data ...interface{}) *Model {
	model := m.getModel()
	if len(data) > 1 {
		if s := gconv.String(data[0]); gstr.Contains(s, "?") {
			model.data = s
			model.extraArgs = data[1:]
		} else {
			m := make(map[string]interface{})
			for i := 0; i < len(data); i += 2 {
				m[gconv.String(data[i])] = data[i+1]
			}
			model.data = m
		}
	} else {
		switch params := data[0].(type) {
		case Result:
			model.data = params.List()
		case Record:
			model.data = params.Map()
		case List:
			list := make(List, len(params))
			for k, v := range params {
				list[k] = gutil.MapCopy(v)
			}
			model.data = list
		case Map:
			model.data = gutil.MapCopy(params)
		default:
			var (
				rv   = reflect.ValueOf(params)
				kind = rv.Kind()
			)
			if kind == reflect.Ptr {
				rv = rv.Elem()
				kind = rv.Kind()
			}
			switch kind {
			case reflect.Slice, reflect.Array:
				list := make(List, rv.Len())
				for i := 0; i < rv.Len(); i++ {
					list[i] = ConvertDataForTableRecord(rv.Index(i).Interface())
				}
				model.data = list
			case reflect.Map:
				model.data = ConvertDataForTableRecord(data[0])
			case reflect.Struct:
				if v, ok := data[0].(apiInterfaces); ok {
					var (
						array = v.Interfaces()
						list  = make(List, len(array))
					)
					for i := 0; i < len(array); i++ {
						list[i] = ConvertDataForTableRecord(array[i])
					}
					model.data = list
				} else {
					model.data = ConvertDataForTableRecord(data[0])
				}
			default:
				model.data = data[0]
			}
		}
	}
	return model
}

// Insert 使用"insert into"语句进行数据库写入，如果写入的数据中存在主键或者唯一索引时，返回失败，否则写入一条新数据。
func (m *Model) Insert(data ...interface{}) (result sql.Result, err error) {
	if len(data) > 0 {
		return m.Data(data...).Insert()
	}
	return m.doInsertWithOption(insertOptionDefault)
}

// InsertIgnore 使用"insert ignore into"语句进行数据库写入，如果写入的数据中存在主键或者唯一索引时，忽略错误继续执行写入。
func (m *Model) InsertIgnore(data ...interface{}) (result sql.Result, err error) {
	if len(data) > 0 {
		return m.Data(data...).InsertIgnore()
	}
	return m.doInsertWithOption(insertOptionIgnore)
}

// Replace 使用"replace into"语句进行数据库写入，如果写入的数据中存在主键或者唯一索引时，会删除原有的记录，必定会写入一条新记录。
func (m *Model) Replace(data ...interface{}) (result sql.Result, err error) {
	if len(data) > 0 {
		return m.Data(data...).Replace()
	}
	return m.doInsertWithOption(insertOptionReplace)
}

// Save 使用"insert into"语句进行数据库写入，如果写入的数据中存在主键或者唯一索引时，更新原有数据，否则写入一条新数据；
func (m *Model) Save(data ...interface{}) (result sql.Result, err error) {
	if len(data) > 0 {
		return m.Data(data...).Save()
	}
	return m.doInsertWithOption(insertOptionSave)
}

// doInsertWithOption 插入带有选项参数的数据。
func (m *Model) doInsertWithOption(option int) (result sql.Result, err error) {
	defer func() {
		if err == nil {
			m.checkAndRemoveCache()
		}
	}()
	if m.data == nil {
		return nil, gerror.New("inserting into table with empty data")
	}
	var (
		nowString       = gtime.Now().String()
		fieldNameCreate = m.getSoftFieldNameCreated()
		fieldNameUpdate = m.getSoftFieldNameUpdated()
		fieldNameDelete = m.getSoftFieldNameDeleted()
	)
	// Batch operation.
	if list, ok := m.data.(List); ok {
		batch := defaultBatchNumber
		if m.batch > 0 {
			batch = m.batch
		}
		newData, err := m.filterDataForInsertOrUpdate(list)
		if err != nil {
			return nil, err
		}
		list = newData.(List)
		// 自动处理创建/更新时间。
		if !m.unscoped && (fieldNameCreate != "" || fieldNameUpdate != "") {
			for k, v := range list {
				gutil.MapDelete(v, fieldNameCreate, fieldNameUpdate, fieldNameDelete)
				if fieldNameCreate != "" {
					v[fieldNameCreate] = nowString
				}
				if fieldNameUpdate != "" {
					v[fieldNameUpdate] = nowString
				}
				list[k] = v
			}
		}
		return m.db.DoBatchInsert(
			m.getLink(true),
			m.tables,
			newData,
			option,
			batch,
		)
	}
	// 单次操作。
	if data, ok := m.data.(Map); ok {
		newData, err := m.filterDataForInsertOrUpdate(data)
		if err != nil {
			return nil, err
		}
		data = newData.(Map)
		// Automatic handling for creating/updating time.
		if !m.unscoped && (fieldNameCreate != "" || fieldNameUpdate != "") {
			gutil.MapDelete(data, fieldNameCreate, fieldNameUpdate, fieldNameDelete)
			if fieldNameCreate != "" {
				data[fieldNameCreate] = nowString
			}
			if fieldNameUpdate != "" {
				data[fieldNameUpdate] = nowString
			}
		}
		return m.db.DoInsert(
			m.getLink(true),
			m.tables,
			newData,
			option,
		)
	}
	return nil, gerror.New("inserting into table with invalid data type")
}
