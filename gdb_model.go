package gdb

import (
	"context"
	"fmt"
	"github.com/gogf/gf/text/gregex"
	"time"

	"github.com/gogf/gf/text/gstr"
)

// Model 是ORM的DAO
type Model struct {
	db            DB             // 底层数据库接口。
	tx            *TX            // 底层事务接口。
	schema        string         // 自定义数据库架构。
	linkType      int            // 主设备或从设备上的操作标记。
	tablesInit    string         // 模型初始化时的表名。
	tables        string         // 操作表名，可以是多个表名和别名，如：“user”、“user u”、“user u、user\u”。
	fields        string         // 操作字段，使用字符'，'连接的多个字段。
	fieldsEx      string         // 排除的操作字段，使用字符'，'连接的多个字段。
	extraArgs     []interface{}  // sql的额外自定义参数。
	whereHolder   []*whereHolder // where操作的条件字符串。
	groupBy       string         // 用于“group by”语句。
	orderBy       string         // 用于“order by”语句。
	having        []interface{}  // 用于“having…”语句。
	start         int            // 用于“select ... start, limit ...”语句。
	limit         int            // 用于“select ... start, limit ...”语句。
	option        int            // Option 额外的操作功能。
	offset        int            // Offset 一些数据库语法的语句。
	data          interface{}    // Data 对于操作，可以是map/[]map/struct/*struct/string等类型。
	batch         int            // Batch 批量插入/替换/保存操作的数量。
	filter        bool           // 根据表的字段过滤数据和where键值对。
	lockInfo      string         // 锁定更新或共享锁定。
	cacheEnabled  bool           // 启用sql结果缓存功能。
	cacheDuration time.Duration  // 缓存TTL持续时间。
	cacheName     string         // 自定义操作的缓存名称。
	unscoped      bool           // 在选择/删除操作时禁用软删除功能。
	safe          bool           // 如果为true，则在操作完成时克隆并返回一个新的模型对象；否则更改当前模型的属性。
}

// whereHolder 是条件准备的持有者。
type whereHolder struct {
	operator int           // Operator for this holder.
	where    interface{}   // Where参数。
	args     []interface{} // where参数的参数。
}

const (
	OptionOmitempty  = 1
	OptionAllowEmpty = 2

	linkTypeMaster   = 1
	linkTypeSlave    = 2
	whereHolderWhere = 1
	whereHolderAnd   = 2
	whereHolderOr    = 3
)

// Table 从给定的模式创建并返回一个新的ORM模型。
// 参数<table>可以是多个表名，也可以是别名，如:
// 1. Table names:
//    Table("user")
//    Table("user u")
//    Table("user, user_detail")
//    Table("user u, user_detail ud")
// 2. Table name with alias: Table("user", "u")
func (c *Core) Table(table ...string) *Model {
	tables := ""
	if len(table) > 1 {
		tables = fmt.Sprintf(
			`%s AS %s`, c.DB.QuotePrefixTableName(table[0]), c.DB.QuoteWord(table[1]),
		)
	} else if len(table) == 1 {
		tables = c.DB.QuotePrefixTableName(table[0])
	} else {
		panic("表不能为空")
	}
	return &Model{
		db:         c.DB,
		tablesInit: tables,
		tables:     tables,
		fields:     "*",
		start:      -1,
		offset:     -1,
		option:     OptionAllowEmpty,
	}
}

// Model 是Core.Table的别名。
func (c *Core) Model(table ...string) *Model {
	return c.DB.Table(table...)
}

// Table 对事务进行操作，返回一个 *gdb.Model对象
func (tx *TX) Table(table ...string) *Model {
	model := tx.db.Table(table...)
	model.db = tx.db
	model.tx = tx
	return model
}

// Model tx.Table的别名。
func (tx *TX) Model(table ...string) *Model {
	return tx.Table(table...)
}

// Ctx 设置当前操作的上下文。
func (m *Model) Ctx(ctx context.Context) *Model {
	if ctx == nil {
		return m
	}
	model := m.getModel()
	model.db = model.db.Ctx(ctx)
	return model
}

// As 设置当前表的别名。
func (m *Model) As(as string) *Model {
	if m.tables != "" {
		model := m.getModel()
		split := " JOIN "
		if gstr.Contains(model.tables, split) {
			// For join table.
			array := gstr.Split(model.tables, split)
			array[len(array)-1], _ = gregex.ReplaceString(`(.+) ON`, fmt.Sprintf(`$1 AS %s ON`, as), array[len(array)-1])
			model.tables = gstr.Join(array, split)
		} else {
			// For base table.
			model.tables = gstr.TrimRight(model.tables) + " AS " + as
		}
		return model
	}
	return m
}

// DB 设置/更改当前操作的db对象。
func (m *Model) DB(db DB) *Model {
	model := m.getModel()
	model.db = db
	return model
}

// TX 设置/更改当前操作的事务。
func (m *Model) TX(tx *TX) *Model {
	model := m.getModel()
	model.db = tx.db
	model.tx = tx
	return model
}

// Schema 设置当前操作的架构。
func (m *Model) Schema(schema string) *Model {
	model := m.getModel()
	model.schema = schema
	return model
}

// Clone 创建并返回一个新模型，该模型是当前模型的克隆。
//
// 请注意，它对克隆使用deep copy。
func (m *Model) Clone() *Model {
	newModel := (*Model)(nil)
	if m.tx != nil {
		newModel = m.tx.Table(m.tablesInit)
	} else {
		newModel = m.db.Table(m.tablesInit)
	}
	*newModel = *m
	// 深度复制切片属性。
	if n := len(m.extraArgs); n > 0 {
		newModel.extraArgs = make([]interface{}, n)
		copy(newModel.extraArgs, m.extraArgs)
	}
	if n := len(m.whereHolder); n > 0 {
		newModel.whereHolder = make([]*whereHolder, n)
		copy(newModel.whereHolder, m.whereHolder)
	}
	return newModel
}

// Master 设置该操作在主节点上执行。
func (m *Model) Master() *Model {
	model := m.getModel()
	model.linkType = linkTypeMaster
	return model
}

// Slave 设置该操作在从节点上执行。
//
// 请注意: 只有在配置了任何从属节点时才有意义。
func (m *Model) Slave() *Model {
	model := m.getModel()
	model.linkType = linkTypeSlave
	return model
}

// Safe 将此模型标记为安全或不安全。如果safe为true，则每当操作完成时，它都会克隆并返回一个新的模型对象，否则它会更改当前模型的属性。
func (m *Model) Safe(safe ...bool) *Model {
	if len(safe) > 0 {
		m.safe = safe[0]
	} else {
		m.safe = true
	}
	return m
}

// Args 为模型操作设置自定义参数。
func (m *Model) Args(args ...interface{}) *Model {
	model := m.getModel()
	model.extraArgs = append(model.extraArgs, args)
	return model
}
