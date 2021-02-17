// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import (
	"bytes"
	"fmt"
	"github.com/gogf/gf/errors/gerror"
	"github.com/gogf/gf/internal/empty"
	"github.com/gogf/gf/internal/json"
	"github.com/gogf/gf/internal/utils"
	"github.com/gogf/gf/os/gtime"
	"github.com/gogf/gf/util/gutil"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/gogf/gf/internal/structs"

	"github.com/gogf/gf/text/gregex"
	"github.com/gogf/gf/text/gstr"
	"github.com/gogf/gf/util/gconv"
)

// apiString 是字符串的类型断言api。
type apiString interface {
	String() string
}

// apiIterator 是迭代器的类型断言api。
type apiIterator interface {
	Iterator(f func(key, value interface{}) bool)
}

// apiInterfaces 是接口的类型断言api。
type apiInterfaces interface {
	Interfaces() []interface{}
}

// apiMapStrAny 是将结构参数转换为map的接口支持。
type apiMapStrAny interface {
	MapStrAny() map[string]interface{}
}

const (
	OrmTagForStruct  = "orm"
	OrmTagForUnique  = "unique"
	OrmTagForPrimary = "primary"
)

var (
	// quoteWordReg 是用于单词检查的正则表达式对象。
	quoteWordReg = regexp.MustCompile(`^[a-zA-Z0-9\-_]+$`)

	// orm字段映射的结构转换的优先级标记。
	structTagPriority = append([]string{OrmTagForStruct}, gconv.StructTagPriority...)
)

// ListItemValues 检索并返回键为<key>的所有item struct/map的元素。
// 请注意，参数<list>应该是包含map或struct元素的slice类型，否则它将返回一个空切片。
//
// 参数<list>支持以下类型:
//
// []map[string]interface{}
//
// []map[string]sub-map
//
// []struct
//
// []struct:sub-struct
//
// 请注意，只有在给定可选参数<subKey>时，sub-map/sub-struct才有意义。
// See gutil.ListItemValues.
func ListItemValues(list interface{}, key interface{}, subKey ...interface{}) (values []interface{}) {
	return gutil.ListItemValues(list, key, subKey...)
}

// ListItemValuesUnique 检索并返回键为<key>的所有struct/map的唯一元素。
// 请注意，参数<list>应该是包含map或struct元素的slice类型，否则它将返回一个空切片。
// See gutil.ListItemValuesUnique.
func ListItemValuesUnique(list interface{}, key string, subKey ...interface{}) []interface{} {
	return gutil.ListItemValuesUnique(list, key, subKey...)
}

// GetInsertOperationByOption 返回具有给定参数<option>的正确insert选项。
func GetInsertOperationByOption(option int) string {
	var operator string
	switch option {
	case insertOptionReplace:
		operator = "REPLACE"
	case insertOptionIgnore:
		operator = "INSERT IGNORE"
	default:
		operator = "INSERT"
	}
	return operator
}

// ConvertDataForTableRecord 是一个非常重要的函数，它对将作为记录插入表中的任何数据进行转换。
//
// 参数<value>应为*map/map/*struct/struct类型，它支持结构的继承结构定义。
func ConvertDataForTableRecord(value interface{}) map[string]interface{} {
	var (
		rvValue reflect.Value
		rvKind  reflect.Kind
		data    = DataToMapDeep(value)
	)
	for k, v := range data {
		rvValue = reflect.ValueOf(v)
		rvKind = rvValue.Kind()
		for rvKind == reflect.Ptr {
			rvValue = rvValue.Elem()
			rvKind = rvValue.Kind()
		}
		switch rvKind {
		case reflect.Slice, reflect.Array, reflect.Map:
			// It should ignore the bytes type.
			if _, ok := v.([]byte); !ok {
				// Convert the value to JSON.
				data[k], _ = json.Marshal(v)
			}
		case reflect.Struct:
			switch v.(type) {
			case time.Time, *time.Time, gtime.Time, *gtime.Time:
				continue
			case Counter, *Counter:
				continue
			default:
				// Use string conversion in default.
				if s, ok := v.(apiString); ok {
					data[k] = s.String()
				} else {
					// Convert the value to JSON.
					data[k], _ = json.Marshal(v)
				}
			}
		}
	}
	return data
}

// DataToMapDeep 递归地将<value>转换为map类型。
// 参数<value>应为*map/map/*struct/struct类型，它支持结构的继承结构定义。
func DataToMapDeep(value interface{}) map[string]interface{} {
	if v, ok := value.(apiMapStrAny); ok {
		return v.MapStrAny()
	}
	var (
		rvValue reflect.Value
		rvField reflect.Value
		rvKind  reflect.Kind
		rtField reflect.StructField
	)
	if v, ok := value.(reflect.Value); ok {
		rvValue = v
	} else {
		rvValue = reflect.ValueOf(value)
	}
	rvKind = rvValue.Kind()
	if rvKind == reflect.Ptr {
		rvValue = rvValue.Elem()
		rvKind = rvValue.Kind()
	}
	// 如果给定的<value>不是结构，则使用gconv.map用于转换。
	if rvKind != reflect.Struct {
		return gconv.Map(value, structTagPriority...)
	}
	// 结构体处理。
	var (
		fieldTag reflect.StructTag
		rvType   = rvValue.Type()
		name     = ""
		data     = make(map[string]interface{})
	)
	for i := 0; i < rvValue.NumField(); i++ {
		rtField = rvType.Field(i)
		rvField = rvValue.Field(i)
		fieldName := rtField.Name
		if !utils.IsLetterUpper(fieldName[0]) {
			continue
		}
		// 结构属性继承
		if rtField.Anonymous {
			for k, v := range DataToMapDeep(rvField) {
				data[k] = v
			}
			continue
		}
		// 其他属性。
		name = ""
		fieldTag = rtField.Tag
		for _, tag := range structTagPriority {
			if s := fieldTag.Get(tag); s != "" {
				name = s
				break
			}
		}
		if name == "" {
			name = fieldName
		} else {
			// “orm”标记支持json tag特性：-，省略为空
			// “orm”tag类似于：“id，priority”，因此应该使用分割处理。
			name = gstr.Trim(name)
			if name == "-" {
				continue
			}
			array := gstr.SplitAndTrim(name, ",")
			if len(array) > 1 {
				switch array[1] {
				case "omitempty":
					if empty.IsEmpty(rvField.Interface()) {
						continue
					} else {
						name = array[0]
					}
				default:
					name = array[0]
				}
			}
		}

		// 底层驱动程序支持时间。time.Time/*time.Time类型。
		fieldValue := rvField.Interface()
		switch fieldValue.(type) {
		case time.Time, *time.Time, gtime.Time, *gtime.Time:
			data[name] = fieldValue
		default:
			// 默认情况下使用字符串转换。
			if s, ok := fieldValue.(apiString); ok {
				data[name] = s.String()
			} else {
				data[name] = fieldValue
			}
		}
	}
	return data
}

// doHandleTableName 为表添加前缀字符串和引号字符。它处理表字符串，如：
//
// "user", "user u", "user,user_detail", "user u, user_detail ut", "user as u, user_detail as ut",
// "user.user u", "`user`.`user` u".
//
// 请注意，这将自动检查表前缀是否已添加，如果为true，则不对表名执行任何操作，否则会将前缀添加到表名。
func doHandleTableName(table, prefix, charLeft, charRight string) string {
	var (
		index  = 0
		chars  = charLeft + charRight
		array1 = gstr.SplitAndTrim(table, ",")
	)
	for k1, v1 := range array1 {
		array2 := gstr.SplitAndTrim(v1, " ")
		// 修剪安全字符。
		array2[0] = gstr.Trim(array2[0], chars)
		// 检查是否有数据库名称。
		array3 := gstr.Split(gstr.Trim(array2[0]), ".")
		for k, v := range array3 {
			array3[k] = gstr.Trim(v, chars)
		}
		index = len(array3) - 1
		// 如果表名已经有前缀，则跳过前缀添加。
		if len(array3[index]) <= len(prefix) || array3[index][:len(prefix)] != prefix {
			array3[index] = prefix + array3[index]
		}
		array2[0] = gstr.Join(array3, ".")
		// 添加安全字符。
		array2[0] = doQuoteString(array2[0], charLeft, charRight)
		array1[k1] = gstr.Join(array2, " ")
	}
	return gstr.Join(array1, ",")
}

// doQuoteWord 检查给定的字符串<s>一个单词，如果为true，则用<charLeft>和<charRight>将其括起来并返回带引号的字符串；否则返回<s>而不做任何更改。
func doQuoteWord(s, charLeft, charRight string) string {
	if quoteWordReg.MatchString(s) && !gstr.ContainsAny(s, charLeft+charRight) {
		return charLeft + s + charRight
	}
	return s
}

// doQuoteString 带引号字符的引号字符串。它处理如下字符串：
// "user",
// "user u",
// "user,user_detail",
// "user u, user_detail ut",
// "user.user u, user.user_detail ut",
// "u.id, u.name, u.age",
// "u.id asc".
func doQuoteString(s, charLeft, charRight string) string {
	array1 := gstr.SplitAndTrim(s, ",")
	for k1, v1 := range array1 {
		array2 := gstr.SplitAndTrim(v1, " ")
		array3 := gstr.Split(gstr.Trim(array2[0]), ".")
		if len(array3) == 1 {
			array3[0] = doQuoteWord(array3[0], charLeft, charRight)
		} else if len(array3) >= 2 {
			array3[0] = doQuoteWord(array3[0], charLeft, charRight)
			// Note:
			// mysql: u.uid
			// mssql double dots: Database..Table
			array3[len(array3)-1] = doQuoteWord(array3[len(array3)-1], charLeft, charRight)
		}
		array2[0] = gstr.Join(array3, ".")
		array1[k1] = gstr.Join(array2, " ")
	}
	return gstr.Join(array1, ",")
}

// GetWhereConditionOfStruct 按给定的结构体指针返回where条件sql和参数。此函数自动检索主字段或唯一字段及其属性值作为条件。
func GetWhereConditionOfStruct(pointer interface{}) (where string, args []interface{}, err error) {
	tagField, err := structs.TagFields(pointer, []string{OrmTagForStruct})
	if err != nil {
		return "", nil, err
	}
	array := ([]string)(nil)
	for _, field := range tagField {
		array = strings.Split(field.TagValue, ",")
		if len(array) > 1 && gstr.InArray([]string{OrmTagForUnique, OrmTagForPrimary}, array[1]) {
			return array[0], []interface{}{field.Value()}, nil
		}
		if len(where) > 0 {
			where += " AND "
		}
		where += field.TagValue + "=?"
		args = append(args, field.Value())
	}
	return
}

// GetPrimaryKey retrieves and returns primary key field name from given struct.
func GetPrimaryKey(pointer interface{}) (string, error) {
	tagField, err := structs.TagFields(pointer, []string{OrmTagForStruct})
	if err != nil {
		return "", err
	}
	array := ([]string)(nil)
	for _, field := range tagField {
		array = strings.Split(field.TagValue, ",")
		if len(array) > 1 && array[1] == OrmTagForPrimary {
			return array[0], nil
		}
	}
	return "", nil
}

// GetPrimaryKeyCondition 按主字段名返回新的where条件。
// 可选参数<where>如下所示:
//
// 123                             => primary=123
//
// []int{1, 2, 3}                  => primary IN(1,2,3)
//
// "john"                          => primary='john'
//
// []string{"john", "smith"}       => primary IN('john','smith')
//
// g.Map{"id": g.Slice{1,2,3}}     => id IN(1,2,3)
//
// g.Map{"id": 1, "name": "john"}  => id=1 AND name='john'
//
//
// 请注意，如果<primary>为空或长度为<where>>1，则直接返回给定的<where>参数。
func GetPrimaryKeyCondition(primary string, where ...interface{}) (newWhereCondition []interface{}) {
	if len(where) == 0 {
		return nil
	}
	if primary == "" {
		return where
	}
	if len(where) == 1 {
		var (
			rv   = reflect.ValueOf(where[0])
			kind = rv.Kind()
		)
		if kind == reflect.Ptr {
			rv = rv.Elem()
			kind = rv.Kind()
		}
		switch kind {
		case reflect.Map, reflect.Struct:
			// Ignore the parameter <primary>.
			break

		default:
			return []interface{}{map[string]interface{}{
				primary: where[0],
			}}
		}
	}
	return where
}

// formatSql 在执行之前格式化sql字符串及其参数。
// 在SQL过程中，内部handleArguments函数可能会被调用两次，但不用担心，它是安全有效的。
func formatSql(sql string, args []interface{}) (newSql string, newArgs []interface{}) {
	// DO NOT do this as there may be multiple lines and comments in the sql.
	// sql = gstr.Trim(sql)
	// sql = gstr.Replace(sql, "\n", " ")
	// sql, _ = gregex.ReplaceString(`\s{2,}`, ` `, sql)
	return handleArguments(sql, args)
}

// formatWhere 格式化where语句及其参数。
func formatWhere(db DB, where interface{}, args []interface{}, omitEmpty bool) (newWhere string, newArgs []interface{}) {
	var (
		buffer = bytes.NewBuffer(nil)
		rv     = reflect.ValueOf(where)
		kind   = rv.Kind()
	)
	if kind == reflect.Ptr {
		rv = rv.Elem()
		kind = rv.Kind()
	}
	switch kind {
	case reflect.Array, reflect.Slice:
		newArgs = formatWhereInterfaces(db, gconv.Interfaces(where), buffer, newArgs)

	case reflect.Map:
		for key, value := range DataToMapDeep(where) {
			if gregex.IsMatchString(regularFieldNameRegPattern, key) && omitEmpty && empty.IsEmpty(value) {
				continue
			}
			newArgs = formatWhereKeyValue(db, buffer, newArgs, key, value)
		}

	case reflect.Struct:
		// 如果<where>struct实现apiterator接口，那么它将使用其Iterate函数来迭代其键值对。
		// 例如，ListMap和TreeMap是有序map，它们实现了apiterator接口，并且对于where条件是索引友好的。
		if iterator, ok := where.(apiIterator); ok {
			iterator.Iterator(func(key, value interface{}) bool {
				ketStr := gconv.String(key)
				if gregex.IsMatchString(regularFieldNameRegPattern, ketStr) && omitEmpty && empty.IsEmpty(value) {
					return true
				}
				newArgs = formatWhereKeyValue(db, buffer, newArgs, ketStr, value)
				return true
			})
			break
		}
		for key, value := range DataToMapDeep(where) {
			if omitEmpty && empty.IsEmpty(value) {
				continue
			}
			newArgs = formatWhereKeyValue(db, buffer, newArgs, key, value)
		}

	default:
		buffer.WriteString(gconv.String(where))
	}

	if buffer.Len() == 0 {
		return "", args
	}
	newArgs = append(newArgs, args...)
	newWhere = buffer.String()
	if len(newArgs) > 0 {
		if gstr.Pos(newWhere, "?") == -1 {
			if gregex.IsMatchString(lastOperatorRegPattern, newWhere) {
				// Eg: Where/And/Or("uid>=", 1)
				newWhere += "?"
			} else if gregex.IsMatchString(regularFieldNameRegPattern, newWhere) {
				newWhere = db.QuoteString(newWhere)
				if len(newArgs) > 0 {
					if utils.IsArray(newArgs[0]) {
						// Eg:
						// Where("id", []int{1,2,3})
						// Where("user.id", []int{1,2,3})
						newWhere += " IN (?)"
					} else if empty.IsNil(newArgs[0]) {
						// Eg:
						// Where("id", nil)
						// Where("user.id", nil)
						newWhere += " IS NULL"
						newArgs = nil
					} else {
						// Eg:
						// Where/And/Or("uid", 1)
						// Where/And/Or("user.uid", 1)
						newWhere += "=?"
					}
				}
			}
		}
	}
	return handleArguments(newWhere, newArgs)
}

// formatWhereInterfaces 将<where>格式化为[]接口{}。
func formatWhereInterfaces(db DB, where []interface{}, buffer *bytes.Buffer, newArgs []interface{}) []interface{} {
	if len(where) == 0 {
		return newArgs
	}
	if len(where)%2 != 0 {
		buffer.WriteString(gstr.Join(gconv.Strings(where), ""))
		return newArgs
	}
	var str string
	for i := 0; i < len(where); i += 2 {
		str = gconv.String(where[i])
		if buffer.Len() > 0 {
			buffer.WriteString(" AND " + db.QuoteWord(str) + "=?")
		} else {
			buffer.WriteString(db.QuoteWord(str) + "=?")
		}
		if s, ok := where[i+1].(Raw); ok {
			buffer.WriteString(gconv.String(s))
		} else {
			newArgs = append(newArgs, where[i+1])
		}
	}
	return newArgs
}

// formatWhereKeyValue 处理参数映射的每个键值对。
func formatWhereKeyValue(db DB, buffer *bytes.Buffer, newArgs []interface{}, key string, value interface{}) []interface{} {
	quotedKey := db.QuoteWord(key)
	if buffer.Len() > 0 {
		buffer.WriteString(" AND ")
	}
	// If the value is type of slice, and there's only one '?' holder in
	// the key string, it automatically adds '?' holder chars according to its arguments count
	// and converts it to "IN" statement.
	var (
		rv   = reflect.ValueOf(value)
		kind = rv.Kind()
	)
	switch kind {
	case reflect.Slice, reflect.Array:
		count := gstr.Count(quotedKey, "?")
		if count == 0 {
			buffer.WriteString(quotedKey + " IN(?)")
			newArgs = append(newArgs, value)
		} else if count != rv.Len() {
			buffer.WriteString(quotedKey)
			newArgs = append(newArgs, value)
		} else {
			buffer.WriteString(quotedKey)
			newArgs = append(newArgs, gconv.Interfaces(value)...)
		}
	default:
		if value == nil || empty.IsNil(rv) {
			if gregex.IsMatchString(regularFieldNameRegPattern, key) {
				// The key is a single field name.
				buffer.WriteString(quotedKey + " IS NULL")
			} else {
				// The key may have operation chars.
				buffer.WriteString(quotedKey)
			}
		} else {
			// It also supports "LIKE" statement, which we considers it an operator.
			quotedKey = gstr.Trim(quotedKey)
			if gstr.Pos(quotedKey, "?") == -1 {
				like := " like"
				if len(quotedKey) > len(like) && gstr.Equal(quotedKey[len(quotedKey)-len(like):], like) {
					// Eg: Where(g.Map{"name like": "john%"})
					buffer.WriteString(quotedKey + " ?")
				} else if gregex.IsMatchString(lastOperatorRegPattern, quotedKey) {
					// Eg: Where(g.Map{"age > ": 16})
					buffer.WriteString(quotedKey + " ?")
				} else if gregex.IsMatchString(regularFieldNameRegPattern, key) {
					// The key is a regular field name.
					buffer.WriteString(quotedKey + "=?")
				} else {
					// The key is not a regular field name.
					// Eg: Where(g.Map{"age > 16": nil})
					// Issue: https://github.com/gogf/gf/issues/765
					if empty.IsEmpty(value) {
						buffer.WriteString(quotedKey)
						break
					} else {
						buffer.WriteString(quotedKey + "=?")
					}
				}
			} else {
				buffer.WriteString(quotedKey)
			}
			if s, ok := value.(Raw); ok {
				buffer.WriteString(gconv.String(s))
			} else {
				newArgs = append(newArgs, value)
			}
		}
	}
	return newArgs
}

// handleArguments 是一个重要的函数，它在将sql及其所有参数提交到底层驱动程序之前处理这些参数。
func handleArguments(sql string, args []interface{}) (newSql string, newArgs []interface{}) {
	newSql = sql
	// insertHolderCount is used to calculate the inserting position for the '?' holder.
	insertHolderCount := 0
	// Handles the slice arguments.
	if len(args) > 0 {
		for index, arg := range args {
			var (
				reflectValue = reflect.ValueOf(arg)
				reflectKind  = reflectValue.Kind()
			)
			for reflectKind == reflect.Ptr {
				reflectValue = reflectValue.Elem()
				reflectKind = reflectValue.Kind()
			}
			switch reflectKind {
			case reflect.Slice, reflect.Array:
				// It does not split the type of []byte.
				// Eg: table.Where("name = ?", []byte("john"))
				if _, ok := arg.([]byte); ok {
					newArgs = append(newArgs, arg)
					continue
				}

				if reflectValue.Len() == 0 {
					// Empty slice argument, it converts the sql to a false sql.
					// Eg:
					// Query("select * from xxx where id in(?)", g.Slice{}) -> select * from xxx where 0=1
					// Where("id in(?)", g.Slice{}) -> WHERE 0=1
					if gstr.Contains(newSql, "?") {
						whereKeyWord := " WHERE "
						if p := gstr.PosI(newSql, whereKeyWord); p == -1 {
							return "0=1", []interface{}{}
						} else {
							return gstr.SubStr(newSql, 0, p+len(whereKeyWord)) + "0=1", []interface{}{}
						}
					}
				} else {
					for i := 0; i < reflectValue.Len(); i++ {
						newArgs = append(newArgs, reflectValue.Index(i).Interface())
					}
				}

				// If the '?' holder count equals the length of the slice,
				// it does not implement the arguments splitting logic.
				// Eg: db.Query("SELECT ?+?", g.Slice{1, 2})
				if len(args) == 1 && gstr.Count(newSql, "?") == reflectValue.Len() {
					break
				}
				// counter is used to finding the inserting position for the '?' holder.
				var (
					counter  = 0
					replaced = false
				)
				newSql, _ = gregex.ReplaceStringFunc(`\?`, newSql, func(s string) string {
					if replaced {
						return s
					}
					counter++
					if counter == index+insertHolderCount+1 {
						replaced = true
						insertHolderCount += reflectValue.Len() - 1
						return "?" + strings.Repeat(",?", reflectValue.Len()-1)
					}
					return s
				})

			// Special struct handling.
			case reflect.Struct:
				switch v := arg.(type) {
				// The underlying driver supports time.Time/*time.Time types.
				case time.Time, *time.Time:
					newArgs = append(newArgs, arg)
					continue

				// Special handling for gtime.Time/*gtime.Time.
				//
				// DO NOT use its underlying gtime.Time.Time as its argument,
				// because the std time.Time will be converted to certain timezone
				// according to underlying driver. And the underlying driver also
				// converts the time.Time to string automatically as the following does.
				case gtime.Time:
					newArgs = append(newArgs, v.String())
					continue

				case *gtime.Time:
					newArgs = append(newArgs, v.String())
					continue

				default:
					// It converts the struct to string in default
					// if it has implemented the String interface.
					if v, ok := arg.(apiString); ok {
						newArgs = append(newArgs, v.String())
						continue
					}
				}
				newArgs = append(newArgs, arg)

			default:
				newArgs = append(newArgs, arg)
			}
		}
	}
	return
}

// formatError 自定义并返回SQL错误。
func formatError(err error, sql string, args ...interface{}) error {
	if err != nil && err != ErrNoRows {
		return gerror.New(fmt.Sprintf("%s, %s\n", err.Error(), FormatSqlWithArgs(sql, args)))
	}
	return err
}

// FormatSqlWithArgs 将参数绑定到sql字符串并返回完整的sql字符串，仅用于调试。
func FormatSqlWithArgs(sql string, args []interface{}) string {
	index := -1
	newQuery, _ := gregex.ReplaceStringFunc(
		`(\?|:v\d+|\$\d+|@p\d+)`, sql, func(s string) string {
			index++
			if len(args) > index {
				if args[index] == nil {
					return "null"
				}
				var (
					rv   = reflect.ValueOf(args[index])
					kind = rv.Kind()
				)
				if kind == reflect.Ptr {
					if rv.IsNil() || !rv.IsValid() {
						return "null"
					}
					rv = rv.Elem()
					kind = rv.Kind()
				}
				switch kind {
				case reflect.String, reflect.Map, reflect.Slice, reflect.Array:
					return `'` + gstr.QuoteMeta(gconv.String(args[index]), `'`) + `'`
				case reflect.Struct:
					if t, ok := args[index].(time.Time); ok {
						return `'` + t.Format(`2006-01-02 15:04:05`) + `'`
					}
					return `'` + gstr.QuoteMeta(gconv.String(args[index]), `'`) + `'`
				}
				return gconv.String(args[index])
			}
			return s
		})
	return newQuery
}

// convertMapToStruct 将<data>映射到给定的结构体。注意，给定的参数<pointer>应该是指向s结构体的指针。
func convertMapToStruct(data map[string]interface{}, pointer interface{}) error {
	tagNameMap, err := structs.TagMapName(pointer, []string{OrmTagForStruct})
	if err != nil {
		return err
	}
	// 它检索并返回orm tagKey和struct属性名称之间的映射。
	mapping := make(map[string]string)
	for tag, attr := range tagNameMap {
		mapping[strings.Split(tag, ",")[0]] = attr
	}
	return gconv.Struct(data, pointer, mapping)
}
