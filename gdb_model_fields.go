// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import (
	"fmt"
	"github.com/gogf/gf/container/gset"
	"github.com/gogf/gf/text/gstr"
	"github.com/gogf/gf/util/gconv"
	"github.com/gogf/gf/util/gutil"
)

// Filter 过滤提交参数中不符合表结构的数据项。请注意: 此函数仅支持单表操作。
func (m *Model) Filter() *Model {
	if gstr.Contains(m.tables, " ") {
		panic("function Filter supports only single table operations")
	}
	model := m.getModel()
	model.filter = true
	return model
}

// Fields 指定需要操作的表字段，包括查询字段、写入字段、更新字段等，多个字段使用字符'，'连接。
//
// 参数<fieldNamesOrMapStruct>的类型可以是string/map/*map/struct/*struct。
func (m *Model) Fields(fieldNamesOrMapStruct ...interface{}) *Model {
	length := len(fieldNamesOrMapStruct)
	if length == 0 {
		return m
	}
	switch {
	// String slice.
	case length >= 2:
		model := m.getModel()
		model.fields = gstr.Join(m.mappingAndFilterToTableFields(gconv.Strings(fieldNamesOrMapStruct)), ",")
		return model
	// It need type asserting.
	case length == 1:
		model := m.getModel()
		switch r := fieldNamesOrMapStruct[0].(type) {
		case string:
			model.fields = gstr.Join(m.mappingAndFilterToTableFields([]string{r}), ",")
		case []string:
			model.fields = gstr.Join(m.mappingAndFilterToTableFields(r), ",")
		default:
			model.fields = gstr.Join(m.mappingAndFilterToTableFields(gutil.Keys(r)), ",")
		}
		return model
	}
	return m
}

// FieldsEx 指定不被操作的表字段, 多个字段使用字符'，'连接。(指定例外的字段，可用于查询字段、写入字段、更新字段等过滤)
//
// 请注意: 此函数仅支持单表操作。参数<fieldNamesOrMapStruct>的类型可以是string/map/*map/struct/*struct。
func (m *Model) FieldsEx(fieldNamesOrMapStruct ...interface{}) *Model {
	length := len(fieldNamesOrMapStruct)
	if length == 0 {
		return m
	}
	model := m.getModel()
	switch {
	case length >= 2:
		model.fieldsEx = gstr.Join(m.mappingAndFilterToTableFields(gconv.Strings(fieldNamesOrMapStruct)), ",")
		return model
	case length == 1:
		switch r := fieldNamesOrMapStruct[0].(type) {
		case string:
			model.fieldsEx = gstr.Join(m.mappingAndFilterToTableFields([]string{r}), ",")
		case []string:
			model.fieldsEx = gstr.Join(m.mappingAndFilterToTableFields(r), ",")
		default:
			model.fieldsEx = gstr.Join(m.mappingAndFilterToTableFields(gutil.Keys(r)), ",")
		}
		return model
	}
	return m
}

// GetFieldsStr 检索并返回表中的所有字段，用字符“，”连接。
//
// 可选参数<prefix>指定每个字段的前缀，例如：FieldsStr（“u”）。
func (m *Model) GetFieldsStr(prefix ...string) string {
	prefixStr := ""
	if len(prefix) > 0 {
		prefixStr = prefix[0]
	}
	tableFields, err := m.db.TableFields(m.tables)
	if err != nil {
		panic(err)
	}
	if len(tableFields) == 0 {
		panic(fmt.Sprintf(`empty table fields for table "%s"`, m.tables))
	}
	fieldsArray := make([]string, len(tableFields))
	for k, v := range tableFields {
		fieldsArray[v.Index] = k
	}
	newFields := ""
	for _, k := range fieldsArray {
		if len(newFields) > 0 {
			newFields += ","
		}
		newFields += prefixStr + k
	}
	newFields = m.db.QuoteString(newFields)
	return newFields
}

// FieldsExStr从表中检索并返回不在参数<fields>中的字段，并用字符'，'连接。参数<fields>指定排除的字段。
//
// 可选参数<prefix>指定每个字段的前缀，例如：FieldsExStr（“id”，“u.”）。
func (m *Model) GetFieldsExStr(fields string, prefix ...string) string {
	prefixStr := ""
	if len(prefix) > 0 {
		prefixStr = prefix[0]
	}
	tableFields, err := m.db.TableFields(m.tables)
	if err != nil {
		panic(err)
	}
	if len(tableFields) == 0 {
		panic(fmt.Sprintf(`empty table fields for table "%s"`, m.tables))
	}
	fieldsExSet := gset.NewStrSetFrom(gstr.SplitAndTrim(fields, ","))
	fieldsArray := make([]string, len(tableFields))
	for k, v := range tableFields {
		fieldsArray[v.Index] = k
	}
	newFields := ""
	for _, k := range fieldsArray {
		if fieldsExSet.Contains(k) {
			continue
		}
		if len(newFields) > 0 {
			newFields += ","
		}
		newFields += prefixStr + k
	}
	newFields = m.db.QuoteString(newFields)
	return newFields
}

// HasField 确定该字段是否存在于表中。
func (m *Model) HasField(field string) (bool, error) {
	tableFields, err := m.db.TableFields(m.tables)
	if err != nil {
		return false, err
	}
	if len(tableFields) == 0 {
		return false, fmt.Errorf(`empty table fields for table "%s"`, m.tables)
	}
	fieldsArray := make([]string, len(tableFields))
	for k, v := range tableFields {
		fieldsArray[v.Index] = k
	}
	for _, f := range fieldsArray {
		if f == field {
			return true, nil
		}
	}
	return false, nil
}
