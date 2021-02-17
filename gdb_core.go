// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.
//

package gdb

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/gogf/gf/errors/gerror"
	"github.com/gogf/gf/text/gstr"
	"reflect"
	"strings"

	"github.com/gogf/gf/internal/utils"

	"github.com/gogf/gf/container/gvar"
	"github.com/gogf/gf/os/gtime"
	"github.com/gogf/gf/text/gregex"
	"github.com/gogf/gf/util/gconv"
)

// Ctx 是一个链接函数，它创建并返回一个新的DB，该DB是当前DB对象的浅层副本，其中包含给定的上下文。
//
// 请注意，返回的DB对象只能使用一次，因此不要将其分配给全局或包变量以供长期使用。
func (c *Core) Ctx(ctx context.Context) DB {
	if ctx == nil {
		return c.DB
	}
	var (
		err        error
		newCore    = &Core{}
		configNode = c.DB.GetConfig()
	)
	*newCore = *c
	newCore.ctx = ctx
	newCore.DB, err = driverMap[configNode.Type].New(newCore, configNode)
	// 很少出错，只需记录。
	if err != nil {
		c.DB.GetLogger().Ctx(ctx).Error(err)
	}
	return newCore.DB
}

// GetCtx 返回当前数据库的上下文。
//
// 如果以前没有设置上下文，它将返回 context.Background()
func (c *Core) GetCtx() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	return context.Background()
}

// GetCtxTimeout 返回指定超时类型的上下文和取消函数。
func (c *Core) GetCtxTimeout(timeoutType int, ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = c.DB.GetCtx()
	} else {
		ctx = context.WithValue(ctx, "WrappedByGetCtxTimeout", nil)
	}
	switch timeoutType {
	case ctxTimeoutTypeExec:
		if c.DB.GetConfig().ExecTimeout > 0 {
			return context.WithTimeout(ctx, c.DB.GetConfig().ExecTimeout)
		}
	case ctxTimeoutTypeQuery:
		if c.DB.GetConfig().QueryTimeout > 0 {
			return context.WithTimeout(ctx, c.DB.GetConfig().QueryTimeout)
		}
	case ctxTimeoutTypePrepare:
		if c.DB.GetConfig().PrepareTimeout > 0 {
			return context.WithTimeout(ctx, c.DB.GetConfig().PrepareTimeout)
		}
	default:
		panic(gerror.Newf("无效的上下文超时类型: %d", timeoutType))
	}
	return ctx, func() {}
}

// Master 如果配置了主从，则从主节点创建并返回连接。如果未配置主从连接，则返回默认连接
func (c *Core) Master() (*sql.DB, error) {
	return c.getSqlDb(true, c.schema.Val())
}

// Slave 如果配置了主从节点，则创建并返回从节点的连接。如果未配置主从连接，则返回默认连接
func (c *Core) Slave() (*sql.DB, error) {
	return c.getSqlDb(false, c.schema.Val())
}

// Query 向基础驱动程序提交一个查询SQL并返回执行结果。它最常用于数据查询。
func (c *Core) Query(sql string, args ...interface{}) (rows *sql.Rows, err error) {
	link, err := c.DB.Slave()
	if err != nil {
		return nil, err
	}
	return c.DB.DoQuery(link, sql, args...)
}

// DoQuery 通过给定的链接对象将sql字符串及其参数提交给底层驱动程序，并返回执行结果。
func (c *Core) DoQuery(link Link, sql string, args ...interface{}) (rows *sql.Rows, err error) {
	sql, args = formatSql(sql, args)
	sql, args = c.DB.HandleSqlBeforeCommit(link, sql, args)
	ctx := c.DB.GetCtx()
	if c.GetConfig().QueryTimeout > 0 {
		var cancelFunc context.CancelFunc
		ctx, cancelFunc = context.WithTimeout(ctx, c.GetConfig().QueryTimeout)
		defer cancelFunc()
	}

	mTime1 := gtime.TimestampMilli()
	rows, err = link.QueryContext(ctx, sql, args...)
	mTime2 := gtime.TimestampMilli()
	sqlObj := &Sql{
		Sql:    sql,
		Type:   "DB.QueryContext",
		Args:   args,
		Format: FormatSqlWithArgs(sql, args),
		Error:  err,
		Start:  mTime1,
		End:    mTime2,
		Group:  c.DB.GetGroup(),
	}
	c.addSqlToTracing(ctx, sqlObj)
	if c.DB.GetDebug() {
		c.writeSqlToLogger(sqlObj)
	}
	if err == nil {
		return rows, nil
	} else {
		err = formatError(err, sql, args...)
	}
	return nil, err
}

// Exec 向基础驱动程序提交一个查询SQL并返回执行结果。它最常用于数据插入和更新。
func (c *Core) Exec(sql string, args ...interface{}) (result sql.Result, err error) {
	link, err := c.DB.Master()
	if err != nil {
		return nil, err
	}
	return c.DB.DoExec(link, sql, args...)
}

// DoExec 通过给定的链接对象将sql字符串及其参数提交给底层驱动程序，并返回执行结果。
func (c *Core) DoExec(link Link, sql string, args ...interface{}) (result sql.Result, err error) {
	sql, args = formatSql(sql, args)
	sql, args = c.DB.HandleSqlBeforeCommit(link, sql, args)
	ctx := c.DB.GetCtx()
	if c.GetConfig().ExecTimeout > 0 {
		var cancelFunc context.CancelFunc
		ctx, cancelFunc = context.WithTimeout(ctx, c.GetConfig().ExecTimeout)
		defer cancelFunc()
	}

	mTime1 := gtime.TimestampMilli()
	if !c.DB.GetDryRun() {
		result, err = link.ExecContext(ctx, sql, args...)
	} else {
		result = new(SqlResult)
	}
	mTime2 := gtime.TimestampMilli()
	sqlObj := &Sql{
		Sql:    sql,
		Type:   "DB.ExecContext",
		Args:   args,
		Format: FormatSqlWithArgs(sql, args),
		Error:  err,
		Start:  mTime1,
		End:    mTime2,
		Group:  c.DB.GetGroup(),
	}
	c.addSqlToTracing(ctx, sqlObj)
	if c.DB.GetDebug() {
		c.writeSqlToLogger(sqlObj)
	}
	return result, formatError(err, sql, args...)
}

// Prepare 预加载: 为以后的查询或执行创建准备好的语句。
//
// 可以从返回的语句同时运行多个查询或执行。
//
// 当不再需要该语句时，调用方必须调用该语句的Close方法。
//
// 参数<execOnMaster>指定是在主节点上执行sql，还是在配置了主从节点的情况下在从节点上执行sql。
func (c *Core) Prepare(sql string, execOnMaster ...bool) (*Stmt, error) {
	var (
		err  error
		link Link
	)
	if len(execOnMaster) > 0 && execOnMaster[0] {
		if link, err = c.DB.Master(); err != nil {
			return nil, err
		}
	} else {
		if link, err = c.DB.Slave(); err != nil {
			return nil, err
		}
	}
	return c.DB.DoPrepare(link, sql)
}

// doPrepare 对给定的链接对象调用prepare函数并返回statement对象。
func (c *Core) DoPrepare(link Link, sql string) (*Stmt, error) {
	ctx := c.DB.GetCtx()
	if c.GetConfig().PrepareTimeout > 0 {
		// DO NOT USE cancel function in prepare statement.
		ctx, _ = context.WithTimeout(ctx, c.GetConfig().PrepareTimeout)
	}
	var (
		mTime1    = gtime.TimestampMilli()
		stmt, err = link.PrepareContext(ctx, sql)
		mTime2    = gtime.TimestampMilli()
		sqlObj    = &Sql{
			Sql:    sql,
			Type:   "DB.PrepareContext",
			Args:   nil,
			Format: FormatSqlWithArgs(sql, nil),
			Error:  err,
			Start:  mTime1,
			End:    mTime2,
			Group:  c.DB.GetGroup(),
		}
	)
	c.addSqlToTracing(ctx, sqlObj)
	if c.DB.GetDebug() {
		c.writeSqlToLogger(sqlObj)
	}
	return &Stmt{
		Stmt: stmt,
		core: c,
		sql:  sql,
	}, err
}

// GetAll 查询并返回数据库中的数据记录。
func (c *Core) All(sql string, args ...interface{}) (Result, error) {
	return c.DB.DoGetAll(nil, sql, args...)
}

// DoGetAll 查询并返回数据库中的数据记录。
func (c *Core) DoGetAll(link Link, sql string, args ...interface{}) (result Result, err error) {
	if link == nil {
		link, err = c.DB.Slave()
		if err != nil {
			return nil, err
		}
	}
	rows, err := c.DB.DoQuery(link, sql, args...)
	if err != nil || rows == nil {
		return nil, err
	}
	defer rows.Close()
	return c.DB.convertRowsToResult(rows)
}

// GetOne 查询并从数据库返回一条记录。
func (c *Core) One(sql string, args ...interface{}) (Record, error) {
	list, err := c.DB.All(sql, args...)
	if err != nil {
		return nil, err
	}
	if len(list) > 0 {
		return list[0], nil
	}
	return nil, nil
}

// GetArray 从数据库中查询并返回数据值作为切片。
//
// 请注意: 如果结果中有多个列，则只随机返回一列值。
func (c *Core) Array(sql string, args ...interface{}) ([]Value, error) {
	all, err := c.DB.DoGetAll(nil, sql, args...)
	if err != nil {
		return nil, err
	}
	return all.Array(), nil
}

// GetStruct 从数据库中查询一条记录并将其转换为给定的结构体。
//
// 参数<pointer>应该是指向struct的指针。
func (c *Core) Struct(pointer interface{}, sql string, args ...interface{}) error {
	one, err := c.DB.One(sql, args...)
	if err != nil {
		return err
	}
	return one.Struct(pointer)
}

// GetStructs 查询数据库中的记录并将其转换为给定的结构。
//
// 参数<pointer>的类型应为struct slice:[]struct/[]*struct。
func (c *Core) Structs(pointer interface{}, sql string, args ...interface{}) error {
	all, err := c.DB.All(sql, args...)
	if err != nil {
		return err
	}
	return all.Structs(pointer)
}

// GetScan 查询数据库中的一个或多个记录，并将它们转换为给定的结构或结构数组。
//
// 如果参数<pointer>是struct pointer的类型，它将在内部调用GetStruct进行转换。
//
// 如果参数<pointer>是片的类型，它将在内部调用GetStructs进行转换。
func (c *Core) Scan(pointer interface{}, sql string, args ...interface{}) error {
	t := reflect.TypeOf(pointer)
	k := t.Kind()
	if k != reflect.Ptr {
		return fmt.Errorf("params should be type of pointer, but got: %v", k)
	}
	k = t.Elem().Kind()
	switch k {
	case reflect.Array, reflect.Slice:
		return c.DB.Structs(pointer, sql, args...)
	case reflect.Struct:
		return c.DB.Struct(pointer, sql, args...)
	}
	return fmt.Errorf("element type should be type of struct/slice, unsupported: %v", k)
}

// GetValue 查询并从数据库返回字段值。
//
// sql应该只从数据库中查询一个字段，否则它只返回结果的一个字段。
func (c *Core) Value(sql string, args ...interface{}) (Value, error) {
	one, err := c.DB.One(sql, args...)
	if err != nil {
		return gvar.New(nil), err
	}
	for _, v := range one {
		return v, nil
	}
	return gvar.New(nil), nil
}

// GetCount 查询并从数据库返回计数。
func (c *Core) Count(sql string, args ...interface{}) (int, error) {
	// 如果查询字段不包含函数“COUNT”，它将替换sql字符串并将“COUNT”函数添加到字段中。
	if !gregex.IsMatchString(`(?i)SELECT\s+COUNT\(.+\)\s+FROM`, sql) {
		sql, _ = gregex.ReplaceString(`(?i)(SELECT)\s+(.+)\s+(FROM)`, `$1 COUNT($2) $3`, sql)
	}
	value, err := c.DB.Value(sql, args...)
	if err != nil {
		return 0, err
	}
	return value.Int(), nil
}

// PingMaster ping主节点以检查身份验证或保持连接活动。
func (c *Core) PingMaster() error {
	if master, err := c.DB.Master(); err != nil {
		return err
	} else {
		return master.Ping()
	}
}

// PingSlave ping从属节点以检查身份验证或使连接保持活动状态。
func (c *Core) PingSlave() error {
	if slave, err := c.DB.Slave(); err != nil {
		return err
	} else {
		return slave.Ping()
	}
}

// Begin 启动并返回事务对象。
//
// 如果不再使用事务，则应调用事务对象的提交或回滚函数。
//
// 提交或回滚函数也会自动关闭事务。
func (c *Core) Begin() (*TX, error) {
	if master, err := c.DB.Master(); err != nil {
		return nil, err
	} else {
		ctx := c.DB.GetCtx()
		if c.GetConfig().TranTimeout > 0 {
			var cancelFunc context.CancelFunc
			ctx, cancelFunc = context.WithTimeout(ctx, c.GetConfig().TranTimeout)
			defer cancelFunc()
		}
		if tx, err := master.BeginTx(ctx, nil); err == nil {
			return &TX{
				db:     c.DB,
				tx:     tx,
				master: master,
			}, nil
		} else {
			return nil, err
		}
	}
}

// Transaction 使用函数<f>包装事务逻辑。
//
// 它回滚事务，如果返回非nil错误，则从函数<f>返回错误，如果函数<f>返回nil，则提交事务并返回nil。
//
//注意: 您不应该在函数<f>中提交或回滚事务，因为它是由该函数自动处理的。
func (c *Core) Transaction(f func(tx *TX) error) (err error) {
	var tx *TX
	tx, err = c.DB.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			if e := recover(); e != nil {
				err = fmt.Errorf("%v", e)
			}
		}
		if err != nil {
			if e := tx.Rollback(); e != nil {
				err = e
			}
		} else {
			if e := tx.Commit(); e != nil {
				err = e
			}
		}
	}()
	err = f(tx)
	return
}

// Insert 对表执行“insert into…”语句。
// 如果表中已经有一个唯一的数据记录，它将返回error。
//
// 参数<data>可以是map/gmap/struct/*struct/[]map/[]struct等类型。
//
// Eg:
//
// Data(g.Map{"uid": 10000, "name":"john"})
//
// Data(g.Slice{g.Map{"uid": 10000, "name":"john"}, g.Map{"uid": 20000, "name":"smith"})
//
// 参数<batch>指定: 给定数据为切片时的批处理操作数量，例如: batch = 10表示一次批处理10条记录。
func (c *Core) Insert(table string, data interface{}, batch ...int) (sql.Result, error) {
	if len(batch) > 0 {
		return c.Model(table).Data(data).Batch(batch[0]).Insert()
	}
	return c.Model(table).Data(data).Insert()
}

// InsertIgnore 对表执行“insert ignore into…”语句。
// 如果表中已经有一个唯一的数据记录，则忽略插入。
//
// 参数<data>可以是map/gmap/struct/*struct/[]map/[]struct等类型。
//
// Eg:
//
// Data(g.Map{"uid": 10000, "name":"john"})
// Data(g.Slice{g.Map{"uid": 10000, "name":"john"}, g.Map{"uid": 20000, "name":"smith"})
//
// 参数<batch>指定: 给定数据为切片时的批处理操作数量，例如: batch = 10表示一次批处理10条记录。
func (c *Core) InsertIgnore(table string, data interface{}, batch ...int) (sql.Result, error) {
	if len(batch) > 0 {
		return c.Model(table).Data(data).Batch(batch[0]).InsertIgnore()
	}
	return c.Model(table).Data(data).InsertIgnore()
}

// Replace 对表执行“replace into…”语句。
// 如果表中已经有一条数据的唯一记录，它将删除该记录并插入一条新记录。
//
// 参数<data>可以是map/gmap/struct/*struct/[]map/[]struct等类型，
// 如果给定的数据是切片类型，那么它将执行批替换，可选参数<batch>指定批操作数量，例如: batch = 10表示一次批处理10条记录。
//
// Eg:
//
// Data(g.Map{"uid": 10000, "name":"john"})
//
// Data(g.Slice{g.Map{"uid": 10000, "name":"john"}, g.Map{"uid": 20000, "name":"smith"})
func (c *Core) Replace(table string, data interface{}, batch ...int) (sql.Result, error) {
	if len(batch) > 0 {
		return c.Model(table).Data(data).Batch(batch[0]).Replace()
	}
	return c.Model(table).Data(data).Replace()
}

// Save 对表执行 "insert into ... on duplicate key update..." 语句。
//
// 如果保存的数据中有主索引或唯一索引，它会更新记录，否则会将新记录插入表中。
//
// 参数<data>可以是map/gmap/struct/*struct/[]map/[]struct等类型。
//
// Eg:
//
// Data(g.Map{"uid": 10000, "name":"john"})
//
// Data(g.Slice{g.Map{"uid": 10000, "name":"john"}, g.Map{"uid": 20000, "name":"smith"})
//
// 如果给定的数据是切片类型，则执行批处理保存，可选参数<batch>指定批处理操作数量，例如: batch = 10表示一次批处理10条记录。
func (c *Core) Save(table string, data interface{}, batch ...int) (sql.Result, error) {
	if len(batch) > 0 {
		return c.Model(table).Data(data).Batch(batch[0]).Save()
	}
	return c.Model(table).Data(data).Save()
}

// doInsert 插入或更新给定表的数据。
// 此函数通常用于自定义接口定义，不需要手动调用。
//
// 参数<data>可以是map/gmap/struct/*struct/[]map/[]struct等类型。
//
// Eg:
//
// Data(g.Map{"uid": 10000, "name":"john"})
//
// Data(g.Slice{g.Map{"uid": 10000, "name":"john"}, g.Map{"uid": 20000, "name":"smith"})
//
// 参数<option>值如下:
//
// 0: insert:  只需插入，如果数据中有唯一/主键，则返回错误；
//
// 1: replace: 如果数据中有唯一/主键，则将其从表中删除并插入一个新的主键；
//
// 2: save:    如果数据中有唯一/主键，它会更新它或插入一个新的；
//
// 3: ignore:  如果数据中有唯一/主键，则忽略插入；
func (c *Core) DoInsert(link Link, table string, data interface{}, option int, batch ...int) (result sql.Result, err error) {
	table = c.DB.QuotePrefixTableName(table)
	var (
		fields       []string
		values       []string
		params       []interface{}
		dataMap      Map
		reflectValue = reflect.ValueOf(data)
		reflectKind  = reflectValue.Kind()
	)
	if reflectKind == reflect.Ptr {
		reflectValue = reflectValue.Elem()
		reflectKind = reflectValue.Kind()
	}
	switch reflectKind {
	case reflect.Slice, reflect.Array:
		return c.DB.DoBatchInsert(link, table, data, option, batch...)
	case reflect.Struct:
		if _, ok := data.(apiInterfaces); ok {
			return c.DB.DoBatchInsert(link, table, data, option, batch...)
		} else {
			dataMap = ConvertDataForTableRecord(data)
		}
	case reflect.Map:
		dataMap = ConvertDataForTableRecord(data)
	default:
		return result, gerror.New(fmt.Sprint("unsupported data type:", reflectKind))
	}
	if len(dataMap) == 0 {
		return nil, gerror.New("data cannot be empty")
	}
	var (
		charL, charR = c.DB.GetChars()
		operation    = GetInsertOperationByOption(option)
		updateStr    = ""
	)
	for k, v := range dataMap {
		fields = append(fields, charL+k+charR)
		if s, ok := v.(Raw); ok {
			values = append(values, gconv.String(s))
		} else {
			values = append(values, "?")
			params = append(params, v)
		}
	}
	if option == insertOptionSave {
		for k, _ := range dataMap {
			// If it's SAVE operation,
			// do not automatically update the creating time.
			if c.isSoftCreatedFiledName(k) {
				continue
			}
			if len(updateStr) > 0 {
				updateStr += ","
			}
			updateStr += fmt.Sprintf(
				"%s%s%s=VALUES(%s%s%s)",
				charL, k, charR,
				charL, k, charR,
			)
		}
		updateStr = fmt.Sprintf("ON DUPLICATE KEY UPDATE %s", updateStr)
	}
	if link == nil {
		if link, err = c.DB.Master(); err != nil {
			return nil, err
		}
	}
	return c.DB.DoExec(
		link,
		fmt.Sprintf(
			"%s INTO %s(%s) VALUES(%s) %s",
			operation, table, strings.Join(fields, ","),
			strings.Join(values, ","), updateStr,
		),
		params...,
	)
}

// BatchInsert 批量插入数据。
// 参数<list>必须是map或struct的切片类型。
func (c *Core) BatchInsert(table string, list interface{}, batch ...int) (sql.Result, error) {
	if len(batch) > 0 {
		return c.Model(table).Data(list).Batch(batch[0]).Insert()
	}
	return c.Model(table).Data(list).Insert()
}

// BatchInsertIgnore 批插入带有忽略选项的数据。
// 参数<list>必须是map或struct的切片类型。
func (c *Core) BatchInsertIgnore(table string, list interface{}, batch ...int) (sql.Result, error) {
	if len(batch) > 0 {
		return c.Model(table).Data(list).Batch(batch[0]).InsertIgnore()
	}
	return c.Model(table).Data(list).InsertIgnore()
}

// BatchReplace 批量替换数据。
// 参数<list>必须是map或struct的切片类型。
func (c *Core) BatchReplace(table string, list interface{}, batch ...int) (sql.Result, error) {
	if len(batch) > 0 {
		return c.Model(table).Data(list).Batch(batch[0]).Replace()
	}
	return c.Model(table).Data(list).Replace()
}

// BatchSave 批量替换数据。
// 参数<list>必须是map或struct的切片类型。
func (c *Core) BatchSave(table string, list interface{}, batch ...int) (sql.Result, error) {
	if len(batch) > 0 {
		return c.Model(table).Data(list).Batch(batch[0]).Save()
	}
	return c.Model(table).Data(list).Save()
}

// DoBatchInsert 批量插入/替换/保存数据。
// 此函数通常用于自定义接口定义，不需要手动调用。
func (c *Core) DoBatchInsert(link Link, table string, list interface{}, option int, batch ...int) (result sql.Result, err error) {
	table = c.DB.QuotePrefixTableName(table)
	var (
		keys    []string      // 字段名。
		values  []string      // 值持有者字符串数组，如：（？,?,?)
		params  []interface{} // 将提交给底层数据库驱动程序的值。
		listMap List          // 从调用者传递的数据列表。
	)
	switch value := list.(type) {
	case Result:
		listMap = value.List()
	case Record:
		listMap = List{value.Map()}
	case List:
		listMap = value
	case Map:
		listMap = List{value}
	default:
		var (
			rv   = reflect.ValueOf(list)
			kind = rv.Kind()
		)
		if kind == reflect.Ptr {
			rv = rv.Elem()
			kind = rv.Kind()
		}
		switch kind {
		// 如果是slice类型，则将其转换为List类型。
		case reflect.Slice, reflect.Array:
			listMap = make(List, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				listMap[i] = ConvertDataForTableRecord(rv.Index(i).Interface())
			}
		case reflect.Map:
			listMap = List{ConvertDataForTableRecord(value)}
		case reflect.Struct:
			if v, ok := value.(apiInterfaces); ok {
				var (
					array = v.Interfaces()
					list  = make(List, len(array))
				)
				for i := 0; i < len(array); i++ {
					list[i] = ConvertDataForTableRecord(array[i])
				}
				listMap = list
			} else {
				listMap = List{ConvertDataForTableRecord(value)}
			}
		default:
			return result, gerror.New(fmt.Sprint("unsupported list type:", kind))
		}
	}
	if len(listMap) < 1 {
		return result, gerror.New("data list cannot be empty")
	}
	if link == nil {
		if link, err = c.DB.Master(); err != nil {
			return
		}
	}
	// 处理字段名和占位符。
	for k, _ := range listMap[0] {
		keys = append(keys, k)
	}
	// 预加载批处理结果指针。
	var (
		charL, charR = c.DB.GetChars()
		batchResult  = new(SqlResult)
		keysStr      = charL + strings.Join(keys, charR+","+charL) + charR
		operation    = GetInsertOperationByOption(option)
		updateStr    = ""
	)
	if option == insertOptionSave {
		for _, k := range keys {
			// 如果是保存操作，不要自动更新创建时间。
			if c.isSoftCreatedFiledName(k) {
				continue
			}
			if len(updateStr) > 0 {
				updateStr += ","
			}
			updateStr += fmt.Sprintf(
				"%s%s%s=VALUES(%s%s%s)",
				charL, k, charR,
				charL, k, charR,
			)
		}
		updateStr = fmt.Sprintf("ON DUPLICATE KEY UPDATE %s", updateStr)
	}
	batchNum := defaultBatchNumber
	if len(batch) > 0 && batch[0] > 0 {
		batchNum = batch[0]
	}
	var (
		listMapLen  = len(listMap)
		valueHolder = make([]string, 0)
	)
	for i := 0; i < listMapLen; i++ {
		values = values[:0]
		// 注意: map类型是无序的，因此应该使用slice+key来检索值。
		for _, k := range keys {
			if s, ok := listMap[i][k].(Raw); ok {
				values = append(values, gconv.String(s))
			} else {
				values = append(values, "?")
				params = append(params, listMap[i][k])
			}
		}
		valueHolder = append(valueHolder, "("+gstr.Join(values, ",")+")")
		if len(valueHolder) == batchNum || (i == listMapLen-1 && len(valueHolder) > 0) {
			r, err := c.DB.DoExec(
				link,
				fmt.Sprintf(
					"%s INTO %s(%s) VALUES%s %s",
					operation, table, keysStr,
					gstr.Join(valueHolder, ","),
					updateStr,
				),
				params...,
			)
			if err != nil {
				return r, err
			}
			if n, err := r.RowsAffected(); err != nil {
				return r, err
			} else {
				batchResult.result = r
				batchResult.affected += n
			}
			params = params[:0]
			valueHolder = valueHolder[:0]
		}
	}
	return batchResult, nil
}

// Update 对表执行 "update ... " 语句。
//
// 参数<data>可以是string/map/gmap/struct/*struct等类型。
//
// Eg:
//
// "uid=10000", "uid", 10000, g.Map{"uid": 10000, "name":"john"}
//
// 参数<condition>可以是string/map/gmap/slice/struct/*struct等类型，它通常与参数<args>一起使用。
//
// Eg:
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
func (c *Core) Update(table string, data interface{}, condition interface{}, args ...interface{}) (sql.Result, error) {
	return c.Model(table).Data(data).Where(condition, args...).Update()
}

// doUpdate 对表执行 "update ... " 语句。
// 此函数通常用于自定义接口定义，不需要手动调用。
func (c *Core) DoUpdate(link Link, table string, data interface{}, condition string, args ...interface{}) (result sql.Result, err error) {
	table = c.DB.QuotePrefixTableName(table)
	var (
		rv   = reflect.ValueOf(data)
		kind = rv.Kind()
	)
	if kind == reflect.Ptr {
		rv = rv.Elem()
		kind = rv.Kind()
	}
	var (
		params  []interface{}
		updates = ""
	)
	switch kind {
	case reflect.Map, reflect.Struct:
		var (
			fields  []string
			dataMap = ConvertDataForTableRecord(data)
		)
		for k, v := range dataMap {
			switch value := v.(type) {
			case *Counter:
				if value.Value != 0 {
					column := c.DB.QuoteWord(value.Field)
					fields = append(fields, fmt.Sprintf("%s=%s+?", column, column))
					params = append(params, value.Value)
				}
			case Counter:
				if value.Value != 0 {
					column := c.DB.QuoteWord(value.Field)
					fields = append(fields, fmt.Sprintf("%s=%s+?", column, column))
					params = append(params, value.Value)
				}
			default:
				if s, ok := v.(Raw); ok {
					fields = append(fields, c.DB.QuoteWord(k)+"="+gconv.String(s))
				} else {
					fields = append(fields, c.DB.QuoteWord(k)+"=?")
					params = append(params, v)
				}

			}
		}
		updates = strings.Join(fields, ",")
	default:
		updates = gconv.String(data)
	}
	if len(updates) == 0 {
		return nil, gerror.New("data cannot be empty")
	}
	if len(params) > 0 {
		args = append(params, args...)
	}
	// If no link passed, it then uses the master link.
	if link == nil {
		if link, err = c.DB.Master(); err != nil {
			return nil, err
		}
	}
	return c.DB.DoExec(
		link,
		fmt.Sprintf("UPDATE %s SET %s%s", table, updates, condition),
		args...,
	)
}

// Delete 对表执行 "delete from ... " 语句。
//
// 参数<condition>可以是string/map/gmap/slice/struct/*struct等类型，它通常与参数<args>一起使用。
//
// Eg:
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
func (c *Core) Delete(table string, condition interface{}, args ...interface{}) (result sql.Result, err error) {
	return c.Model(table).Where(condition, args...).Delete()
}

// DoDelete 对表执行 "delete from ... " 语句。
// 此函数通常用于自定义接口定义，不需要手动调用。
func (c *Core) DoDelete(link Link, table string, condition string, args ...interface{}) (result sql.Result, err error) {
	if link == nil {
		if link, err = c.DB.Master(); err != nil {
			return nil, err
		}
	}
	table = c.DB.QuotePrefixTableName(table)
	return c.DB.DoExec(link, fmt.Sprintf("DELETE FROM %s%s", table, condition), args...)
}

// convertRowsToResult 转换基础数据记录类型sql.行到结果类型。
func (c *Core) convertRowsToResult(rows *sql.Rows) (Result, error) {
	if !rows.Next() {
		return nil, nil
	}
	// Column names and types.
	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	columnTypes := make([]string, len(columns))
	columnNames := make([]string, len(columns))
	for k, v := range columns {
		columnTypes[k] = v.DatabaseTypeName()
		columnNames[k] = v.Name()
	}
	var (
		values   = make([]interface{}, len(columnNames))
		records  = make(Result, 0)
		scanArgs = make([]interface{}, len(values))
	)
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for {
		if err := rows.Scan(scanArgs...); err != nil {
			return records, err
		}
		row := make(Record)
		for i, value := range values {
			if value == nil {
				row[columnNames[i]] = gvar.New(nil)
			} else {
				row[columnNames[i]] = gvar.New(c.DB.convertFieldValueToLocalValue(value, columnTypes[i]))
			}
		}
		records = append(records, row)
		if !rows.Next() {
			break
		}
	}
	return records, nil
}

// MarshalJSON 为json.Marshal实现MarshalJSON接口，它只返回指针地址。
//
// 注意，这个接口的实现主要是为了解决Golang版本<v1.14的json无限循环错误。
func (c *Core) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`%+v`, c)), nil
}

// writeSqlToLogger 将sql对象输出到记录器。它仅当配置“debug”为真时才启用。
func (c *Core) writeSqlToLogger(v *Sql) {
	s := fmt.Sprintf("[%3d ms] [%s] %s", v.End-v.Start, v.Group, v.Format)
	if v.Error != nil {
		s += "\nError: " + v.Error.Error()
		c.logger.Ctx(c.DB.GetCtx()).Error(s)
	} else {
		c.logger.Ctx(c.DB.GetCtx()).Debug(s)
	}
}

// HasTable 确定数据库中是否存在表名。
func (c *Core) HasTable(name string) (bool, error) {
	tableList, err := c.DB.Tables()
	if err != nil {
		return false, err
	}
	for _, table := range tableList {
		if table == name {
			return true, nil
		}
	}
	return false, nil
}

// isSoftCreatedFiledName 检查并返回给定的文件名是否是自动填充的创建时间。
func (c *Core) isSoftCreatedFiledName(fieldName string) bool {
	if fieldName == "" {
		return false
	}
	if config := c.DB.GetConfig(); config.CreatedAt != "" {
		if utils.EqualFoldWithoutChars(fieldName, config.CreatedAt) {
			return true
		}
		return gstr.InArray(append([]string{config.CreatedAt}, createdFiledNames...), fieldName)
	}
	for _, v := range createdFiledNames {
		if utils.EqualFoldWithoutChars(fieldName, v) {
			return true
		}
	}
	return false
}
