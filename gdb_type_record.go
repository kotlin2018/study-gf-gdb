// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import (
	"database/sql"
	"github.com/gogf/gf/container/gmap"
	"github.com/gogf/gf/encoding/gparser"
	"github.com/gogf/gf/errors/gerror"
	"github.com/gogf/gf/internal/empty"
	"github.com/gogf/gf/util/gconv"
	"reflect"
)

// Json 将<r>转换为JSON格式的内容。
func (r Record) Json() string {
	content, _ := gparser.VarToJson(r.Map())
	return gconv.UnsafeBytesToStr(content)
}

// Xml 将<r>转换为XML格式的内容。
func (r Record) Xml(rootTag ...string) string {
	content, _ := gparser.VarToXml(r.Map(), rootTag...)
	return gconv.UnsafeBytesToStr(content)
}

// Map 将<r>转换为map[string]interface{}
func (r Record) Map() Map {
	m := make(map[string]interface{})
	for k, v := range r {
		m[k] = v.Val()
	}
	return m
}

// GMap 将<r>转换为gmap
func (r Record) GMap() *gmap.StrAnyMap {
	return gmap.NewStrAnyMapFrom(r.Map())
}

// Struct 将<r>转换为结构体，参数<pointer>的类型应该是*struct/**struct。
//
//
// 注意: 如果<r>为空，它返回sql.ErrNoRows
func (r Record) Struct(pointer interface{}) error {
	// If the record is empty, it returns error.
	if r.IsEmpty() {
		if !empty.IsNil(pointer, true) {
			return sql.ErrNoRows
		}
		return nil
	}
	// Special handling for parameter type: reflect.Value
	if _, ok := pointer.(reflect.Value); ok {
		return convertMapToStruct(r.Map(), pointer)
	}
	var (
		reflectValue = reflect.ValueOf(pointer)
		reflectKind  = reflectValue.Kind()
	)
	if reflectKind != reflect.Ptr {
		return gerror.New("parameter should be type of *struct/**struct")
	}
	reflectValue = reflectValue.Elem()
	reflectKind = reflectValue.Kind()
	if reflectKind == reflect.Invalid {
		return gerror.New("parameter is an invalid pointer, maybe nil")
	}
	if reflectKind != reflect.Ptr && reflectKind != reflect.Struct {
		return gerror.New("parameter should be type of *struct/**struct")
	}
	return convertMapToStruct(r.Map(), pointer)
}

// IsEmpty 检查并返回<r>是否为空。
func (r Record) IsEmpty() bool {
	return len(r) == 0
}
