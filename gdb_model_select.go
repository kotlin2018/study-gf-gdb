// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import (
	"fmt"
	"github.com/gogf/gf/container/gset"
	"github.com/gogf/gf/container/gvar"
	"github.com/gogf/gf/internal/intlog"
	"github.com/gogf/gf/internal/json"
	"github.com/gogf/gf/text/gstr"
	"github.com/gogf/gf/util/gconv"
	"reflect"
)

// All 对model执行“select from...”语句，它从表中检索记录，并以切片类型返回结果。
//
// 如果没有使用表中给定的条件检索到记录，则返回nil。
//
// 可选参数<where>与Model.Where()的参数相同
func (m *Model) All(where ...interface{}) (Result, error) {
	return m.doGetAll(false, where...)
}

// doGetAll 对model执行“select from...”语句，它从表中检索记录，并以切片类型返回结果。
//
// 如果没有使用表中给定的条件检索到记录，则返回nil。
//
// 参数<limit1>指定如果未设置m.limit，是否限制只查询一条记录。
//
// 可选参数<where>与Model.Where()的参数相同。
func (m *Model) doGetAll(limit1 bool, where ...interface{}) (Result, error) {
	if len(where) > 0 {
		return m.Where(where[0], where[1:]...).All()
	}
	var (
		softDeletingCondition                         = m.getConditionForSoftDeleting()
		conditionWhere, conditionExtra, conditionArgs = m.formatCondition(limit1, false)
	)
	if !m.unscoped && softDeletingCondition != "" {
		if conditionWhere == "" {
			conditionWhere = " WHERE "
		} else {
			conditionWhere += " AND "
		}
		conditionWhere += softDeletingCondition
	}

	// DO NOT quote the m.fields where, in case of fields like:
	// DISTINCT t.user_id uid
	return m.doGetAllBySql(
		fmt.Sprintf(
			"SELECT %s FROM %s%s",
			m.getFieldsFiltered(),
			m.tables,
			conditionWhere+conditionExtra,
		),
		conditionArgs...,
	)
}

// getFieldsFiltered 检查字段和fieldsEx属性，筛选并返回将真正提交给底层数据库驱动程序的字段。
func (m *Model) getFieldsFiltered() string {
	if m.fieldsEx == "" {
		// No filtering.
		if !gstr.Contains(m.fields, ".") && !gstr.Contains(m.fields, " ") {
			return m.db.QuoteString(m.fields)
		}
		return m.fields
	}
	var (
		fieldsArray []string
		fieldsExSet = gset.NewStrSetFrom(gstr.SplitAndTrim(m.fieldsEx, ","))
	)
	if m.fields != "*" {
		// Filter custom fields with fieldEx.
		fieldsArray = make([]string, 0, 8)
		for _, v := range gstr.SplitAndTrim(m.fields, ",") {
			fieldsArray = append(fieldsArray, v[gstr.PosR(v, "-")+1:])
		}
	} else {
		if gstr.Contains(m.tables, " ") {
			panic("function FieldsEx supports only single table operations")
		}
		// Filter table fields with fieldEx.
		tableFields, err := m.db.TableFields(m.tables)
		if err != nil {
			panic(err)
		}
		if len(tableFields) == 0 {
			panic(fmt.Sprintf(`empty table fields for table "%s"`, m.tables))
		}
		fieldsArray = make([]string, len(tableFields))
		for k, v := range tableFields {
			fieldsArray[v.Index] = k
		}
	}
	newFields := ""
	for _, k := range fieldsArray {
		if fieldsExSet.Contains(k) {
			continue
		}
		if len(newFields) > 0 {
			newFields += ","
		}
		newFields += m.db.QuoteWord(k)
	}
	return newFields
}

// Chunk 使用给定的大小和回调函数迭代查询结果。
func (m *Model) Chunk(limit int, callback func(result Result, err error) bool) {
	page := m.start
	if page <= 0 {
		page = 1
	}
	model := m
	for {
		model = model.Page(page, limit)
		data, err := model.All()
		if err != nil {
			callback(nil, err)
			break
		}
		if len(data) == 0 {
			break
		}
		if callback(data, err) == false {
			break
		}
		if len(data) < limit {
			break
		}
		page++
	}
}

// One 从表中检索一条记录并将结果作为map类型返回;
//
// 如果没有使用表中给定的条件检索到记录，则返回nil;
//
// 可选参数<where>与Model.Where()的参数相同。
func (m *Model) One(where ...interface{}) (Record, error) {
	if len(where) > 0 {
		return m.Where(where[0], where[1:]...).One()
	}
	all, err := m.doGetAll(true)
	if err != nil {
		return nil, err
	}
	if len(all) > 0 {
		return all[0], nil
	}
	return nil, nil
}

// Value 从表中查询并返回一个字段的值，并将结果作为接口类型返回，往往需要结合Fields方法使用。
//
// 如果在表中找不到具有给定条件的记录，则返回nil。
//
// 如果提供了可选参数<fieldsAndWhere>，则fieldsAndWhere[0]是所选字段，fieldsAndWhere[1:]被视为where条件字段。
func (m *Model) Value(fieldsAndWhere ...interface{}) (Value, error) {
	if len(fieldsAndWhere) > 0 {
		if len(fieldsAndWhere) > 2 {
			return m.Fields(gconv.String(fieldsAndWhere[0])).Where(fieldsAndWhere[1], fieldsAndWhere[2:]...).Value()
		} else if len(fieldsAndWhere) == 2 {
			return m.Fields(gconv.String(fieldsAndWhere[0])).Where(fieldsAndWhere[1]).Value()
		} else {
			return m.Fields(gconv.String(fieldsAndWhere[0])).Value()
		}
	}
	one, err := m.One()
	if err != nil {
		return gvar.New(nil), err
	}
	for _, v := range one {
		return v, nil
	}
	return gvar.New(nil), nil
}

// Array 用于查询指定字段列的数据，返回数组。请注意: 如果结果中有多个列，它将随机返回一个列值。
//
// 如果提供了可选参数<fieldsAndWhere>，则fieldsAndWhere[0]是所选字段，fieldsAndWhere[1:]被视为where条件字段。
func (m *Model) Array(fieldsAndWhere ...interface{}) ([]Value, error) {
	if len(fieldsAndWhere) > 0 {
		if len(fieldsAndWhere) > 2 {
			return m.Fields(gconv.String(fieldsAndWhere[0])).Where(fieldsAndWhere[1], fieldsAndWhere[2:]...).Array()
		} else if len(fieldsAndWhere) == 2 {
			return m.Fields(gconv.String(fieldsAndWhere[0])).Where(fieldsAndWhere[1]).Array()
		} else {
			return m.Fields(gconv.String(fieldsAndWhere[0])).Array()
		}
	}
	all, err := m.All()
	if err != nil {
		return nil, err
	}
	return all.Array(), nil
}

// Struct 从表中检索一条记录并将其转换为给定的结构体，
// 参数<pointer>的类型应为*struct/**struct。如果给定了**struct类型，它可以在转换过程中在内部创建结构;
//
// 可选参数<where>与Model.Where()的参数相同;
//
// 注意: 如果<pointer>不是nil，并且使用给定的条件没有检索到表中的记录，返回sql.ErrNoRows。
//
// Eg:
//
// user := new(User)  err := db.Model("user").Where("id", 1).Struct(user)
//
// user := (*User)(nil) err := db.Model("user").Where("id", 1).Struct(&user)
func (m *Model) Struct(pointer interface{}, where ...interface{}) error {
	one, err := m.One(where...)
	if err != nil {
		return err
	}
	return one.Struct(pointer)
}

// Structs 从表中检索记录并将其转换为给定的结构体切片，
// 参数<pointer>的类型应为*[]struct/*[]*struct。它可以在转换期间在内部创建和填充结构片;
//
// 可选参数<where>与Model.Where()的参数相同;
//
// 注意: 如果<pointer>不是nil，并且使用给定的条件没有检索到表中的记录，返回sql.ErrNoRows。
//
// Eg:
//
// users := ([]User)(nil) err := db.Model("user").Structs(&users);
//
// users := ([]*User)(nil) err := db.Model("user").Structs(&users)
func (m *Model) Structs(pointer interface{}, where ...interface{}) error {
	all, err := m.All(where...)
	if err != nil {
		return err
	}
	return all.Structs(pointer)
}

// Scan 根据参数<pointer>的类型自动调用Struct或Structs函数。
//
// 如果<pointer>是*Struct/**Struct类型，则调用函数Struct。
//
// 如果<pointer>的类型为*[]struct/*[]*struct，则调用函数Structs。
//
// 可选参数<where>与Model.Where()的参数相同。
//
// 注意: 如果<pointer>不是nil，并且使用给定的条件没有检索到表中的记录，返回sql.ErrNoRows。
//
// Eg:
//
// user := new(User) err := db.Model("user").Where("id", 1).Scan(user)；
//
// user := (*User)(nil) err := db.Model("user").Where("id", 1).Scan(&user)；
//
// users := ([]User)(nil) err := db.Model("user").Scan(&users)；
//
// users := ([]*User)(nil) err := db.Model("user").Scan(&users)
func (m *Model) Scan(pointer interface{}, where ...interface{}) error {
	t := reflect.TypeOf(pointer)
	k := t.Kind()
	if k != reflect.Ptr {
		return fmt.Errorf("params should be type of pointer, but got: %v", k)
	}
	switch t.Elem().Kind() {
	case reflect.Array, reflect.Slice:
		return m.Structs(pointer, where...)
	default:
		return m.Struct(pointer, where...)
	}
}

// ScanList 将<r>转换为包含其他复杂结构属性的结构体切片。
// 请注意: 参数<listPointer>的类型应为*[]struct/*[]*struct。
//
// 用法示例:
//
// type Entity struct {
// 	   User       *EntityUser
// 	   UserDetail *EntityUserDetail
//	   UserScores []*EntityUserScores
// }
//
// var users []*Entity 或者 var users []Entity
//
// ScanList(&users, "User")；
//
// ScanList(&users, "UserDetail", "User", "uid:Uid")；
//
// ScanList(&users, "UserScores", "User", "uid:Uid")；
//
// 示例代码中的参数“User”/“UserDetail”/“UserScores”指定当前结果将绑定到的目标属性结构。
//
// 示例代码中的“uid”是结果的表字段名，“uid”是关系结构体属性名。
//
// 它自动计算具有给定<relation>参数的HasOne/HasMany关系。
//
// 请参阅示例或单元测试用例，以获得对此函数的清晰理解。
func (m *Model) ScanList(listPointer interface{}, attributeName string, relation ...string) (err error) {
	all, err := m.All()
	if err != nil {
		return err
	}
	return all.ScanList(listPointer, attributeName, relation...)
}

// Count 对Model执行 "select count(x) from ..."语句。
// 可选参数<where>与Model.Where()的参数相同。
func (m *Model) Count(where ...interface{}) (int, error) {
	if len(where) > 0 {
		return m.Where(where[0], where[1:]...).Count()
	}
	countFields := "COUNT(1)"
	if m.fields != "" && m.fields != "*" {
		// DO NOT quote the m.fields here, in case of fields like:
		// DISTINCT t.user_id uid
		countFields = fmt.Sprintf(`COUNT(%s)`, m.fields)
	}
	var (
		softDeletingCondition                         = m.getConditionForSoftDeleting()
		conditionWhere, conditionExtra, conditionArgs = m.formatCondition(false, true)
	)
	if !m.unscoped && softDeletingCondition != "" {
		if conditionWhere == "" {
			conditionWhere = " WHERE "
		} else {
			conditionWhere += " AND "
		}
		conditionWhere += softDeletingCondition
	}

	s := fmt.Sprintf("SELECT %s FROM %s%s", countFields, m.tables, conditionWhere+conditionExtra)
	if len(m.groupBy) > 0 {
		s = fmt.Sprintf("SELECT COUNT(1) FROM (%s) count_alias", s)
	}
	list, err := m.doGetAllBySql(s, conditionArgs...)
	if err != nil {
		return 0, err
	}
	if len(list) > 0 {
		for _, v := range list[0] {
			return v.Int(), nil
		}
	}
	return 0, nil
}

// FindOne 通过M.WherePri和M.One检索并返回单个记录。
func (m *Model) FindOne(where ...interface{}) (Record, error) {
	if len(where) > 0 {
		return m.WherePri(where[0], where[1:]...).One()
	}
	return m.One()
}

// FindAll 通过M.WherePri和M.All检索并返回结果集。
func (m *Model) FindAll(where ...interface{}) (Result, error) {
	if len(where) > 0 {
		return m.WherePri(where[0], where[1:]...).All()
	}
	return m.All()
}

// FindValue 通过Model.WherePri和Model.Value检索并返回单个记录。
func (m *Model) FindValue(fieldsAndWhere ...interface{}) (Value, error) {
	if len(fieldsAndWhere) >= 2 {
		return m.WherePri(fieldsAndWhere[1], fieldsAndWhere[2:]...).Fields(gconv.String(fieldsAndWhere[0])).Value()
	}
	if len(fieldsAndWhere) == 1 {
		return m.Fields(gconv.String(fieldsAndWhere[0])).Value()
	}
	return m.Value()
}

// FindArray 从数据库中查询并返回[]Value。请注意: 如果结果中有多个列，它将随机返回一个列值。
//
// 详情请看: Model.WherePri 和 Model.Value.
func (m *Model) FindArray(fieldsAndWhere ...interface{}) ([]Value, error) {
	if len(fieldsAndWhere) >= 2 {
		return m.WherePri(fieldsAndWhere[1], fieldsAndWhere[2:]...).Fields(gconv.String(fieldsAndWhere[0])).Array()
	}
	if len(fieldsAndWhere) == 1 {
		return m.Fields(gconv.String(fieldsAndWhere[0])).Array()
	}
	return m.Array()
}

// FindCount 通过 Model.WherePri 和 Model.Count，检索并返回记录数
func (m *Model) FindCount(where ...interface{}) (int, error) {
	if len(where) > 0 {
		return m.WherePri(where[0], where[1:]...).Count()
	}
	return m.Count()
}

// FindScan 通过 Model.WherePri and Model.Scan 检索并返回record/records
func (m *Model) FindScan(pointer interface{}, where ...interface{}) error {
	if len(where) > 0 {
		return m.WherePri(where[0], where[1:]...).Scan(pointer)
	}
	return m.Scan(pointer)
}

// doGetAllBySql 对数据库执行select语句。
func (m *Model) doGetAllBySql(sql string, args ...interface{}) (result Result, err error) {
	cacheKey := ""
	cacheObj := m.db.GetCache().Ctx(m.db.GetCtx())
	// Retrieve from cache.
	if m.cacheEnabled && m.tx == nil {
		cacheKey = m.cacheName
		if len(cacheKey) == 0 {
			cacheKey = sql + ", @PARAMS:" + gconv.String(args)
		}
		if v, _ := cacheObj.GetVar(cacheKey); !v.IsNil() {
			if result, ok := v.Val().(Result); ok {
				// In-memory cache.
				return result, nil
			} else {
				// Other cache, it needs conversion.
				var result Result
				if err = json.Unmarshal(v.Bytes(), &result); err != nil {
					return nil, err
				} else {
					return result, nil
				}
			}
		}
	}
	result, err = m.db.DoGetAll(m.getLink(false), sql, m.mergeArguments(args)...)
	// Cache the result.
	if cacheKey != "" && err == nil {
		if m.cacheDuration < 0 {
			if _, err := cacheObj.Remove(cacheKey); err != nil {
				intlog.Error(err)
			}
		} else {
			if err := cacheObj.Set(cacheKey, result, m.cacheDuration); err != nil {
				intlog.Error(err)
			}
		}
	}
	return result, err
}
