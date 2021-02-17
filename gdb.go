// gdb包: 为流行的关系数据库提供ORM特性
package study_gf_gdb

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/gogf/gf/errors/gerror"
	"github.com/gogf/gf/os/gcmd"
	"time"

	"github.com/gogf/gf/container/gvar"

	"github.com/gogf/gf/os/glog"

	"github.com/gogf/gf/container/gmap"
	"github.com/gogf/gf/container/gtype"
	"github.com/gogf/gf/os/gcache"
	"github.com/gogf/gf/util/grand"
)

// DB接口是数据库操作的核心接口，也是我们通过ORM操作数据库时最常用的接口。
//
// DB接口不仅是为关系数据库设计的，而且将来也为NoSQL数据库设计的，The name "Table" 已经不适合这个目的了。
type DB interface {

	// 用于创建指定数据表的Model对象。
	//
	// 示例:
	//
	// 数据库有一张 "user"表
	//
	// m := g.DB().Table("user")
	//
	// m := g.DB().Model("user")
	Table(table ...string) *Model
	// 用于创建指定数据表的Model对象。
	//
	// 示例:
	//
	// 数据库有一张 "user"表
	//
	// m := g.DB().Table("user") 等价于 m := g.DB().Model("user");
	//
	// m := g.DB().Model("user")  m.DB(g.DB("user-center"))
	//
	// 等价于
	// m := g.DB("user-center").Model("user")
	Model(table ...string) *Model
	// Schema返回一个模式对象,用于切换数据库。
	Schema(schema string) *Schema

	// Open (被数据库驱动实现)为具有给定节点配置的数据库创建原始连接对象，返回一个数据库操作句柄。
	// 请注意，不建议手动使用此功能.
	Open(config *ConfigNode) (*sql.DB, error)

	// ORM支持传递自定义的context上下文变量，用于异步IO控制或者上下文信息传递，特别是链路跟踪信息的传递，
	//
	// 我们可以通过Ctx方法传递自定义的上下文变量给ORM对象，Ctx方法其实是一个链式操作方法，该上下文传递进去后仅对当前DB接口对象有效。
	//
	// 请注意，返回的DB对象只能使用一次，因此不要将其分配给全局或包变量以供长期使用。
	Ctx(ctx context.Context) DB

	// 查询操作
	//
	// SQL操作方法，返回原生的标准库sql对象
	Query(sql string, args ...interface{}) (*sql.Rows, error)
	// SQL操作方法，返回原生的标准库sql对象。
	//
	// 对表执行: 增加/删除/更新操作 (不包括查询操作)，只支持 CUD SQL语句。
	Exec(sql string, args ...interface{}) (sql.Result, error)
	// SQL操作方法，返回原生的标准库sql对象。
	//
	// 为以后的查询(Query)或执行(Exec)创建准备好的语句，即:预加载原生的CRUD SQL语句。
	//
	// 可以从返回的语句同时运行多个查询(Query)或执行(Exec)，当不再需要该语句时，调用方必须调用该语句的Close方法。
	Prepare(sql string, execOnMaster ...bool) (*Stmt, error)

	// 插入一条记录到数据库表中；
	//
	// 使用 "insert into" 语句进行数据库写入，如果写入的数据中存在主键或者唯一索引时，返回失败，否则写入一条新数据。
	Insert(table string, data interface{}, batch ...int) (sql.Result, error)
	// 插入一条记录到数据库表中；
	//
	// 使用 "insert ignore into" 语句进行数据库写入，如果写入的数据中存在主键或者唯一索引时，忽略错误继续执行写入。
	InsertIgnore(table string, data interface{}, batch ...int) (sql.Result, error)
	// 替换数据库表中的一条记录；
	//
	// 使用 "replace into" 语句进行数据库写入，如果写入的数据中存在主键或者唯一索引时，会删除原有的记录，必定会写入一条新记录。
	Replace(table string, data interface{}, batch ...int) (sql.Result, error)
	// 保存一条记录到数据库表中；
	//
	// 使用 "insert into语句" 进行数据库写入，如果写入的数据中存在主键或者唯一索引时，更新原有数据，否则写入一条新数据；
	Save(table string, data interface{}, batch ...int) (sql.Result, error)

	// 数据批量插入到数据库表中。
	BatchInsert(table string, list interface{}, batch ...int) (sql.Result, error)
	// 数据批量替换数据库表中的记录。
	BatchReplace(table string, list interface{}, batch ...int) (sql.Result, error)
	// 数据批量保存到数据库表中
	BatchSave(table string, list interface{}, batch ...int) (sql.Result, error)
	// 更新数据
	//
	// 该方法必须带有Where条件才能提交执行，否则将会返回错误
	Update(table string, data interface{}, condition interface{}, args ...interface{}) (sql.Result, error)
	// 物理删除数据（数据硬删除），删除的数据不可恢复，请慎重使用该方法。
	//
	// 该方法必须带有Where条件才能提交执行，否则将会返回错误。
	Delete(table string, condition interface{}, args ...interface{}) (sql.Result, error)

	// Do* 系列方法是给底层驱动调用的。
	//
	// 它的第一个参数link为Link接口对象，该对象在master-slave模式下可能是一个主节点对象，也可能是从节点对象，
	// 因此如果在继承的驱动对象实现中使用该link接口对象时，注意当前的运行模式(slave节点在大部分的数据库主从模式中往往是不可写的)。
	DoQuery(link Link, sql string, args ...interface{}) (rows *sql.Rows, err error)
	// Do* 系列方法是给底层驱动调用的。
	//
	// 它的第一个参数link为Link接口对象，该对象在master-slave模式下可能是一个主节点对象，也可能是从节点对象，
	// 因此如果在继承的驱动对象实现中使用该link接口对象时，注意当前的运行模式(slave节点在大部分的数据库主从模式中往往是不可写的)。
	DoGetAll(link Link, sql string, args ...interface{}) (result Result, err error)
	// Do* 系列方法是给底层驱动调用的。
	//
	// 它的第一个参数link为Link接口对象，该对象在master-slave模式下可能是一个主节点对象，也可能是从节点对象，
	// 因此如果在继承的驱动对象实现中使用该link接口对象时，注意当前的运行模式(slave节点在大部分的数据库主从模式中往往是不可写的)。
	DoExec(link Link, sql string, args ...interface{}) (result sql.Result, err error)
	// Do* 系列方法是给底层驱动调用的。
	//
	// 它的第一个参数link为Link接口对象，该对象在master-slave模式下可能是一个主节点对象，也可能是从节点对象，
	// 因此如果在继承的驱动对象实现中使用该link接口对象时，注意当前的运行模式(slave节点在大部分的数据库主从模式中往往是不可写的)。
	DoPrepare(link Link, sql string) (*Stmt, error)
	// Do* 系列方法是给底层驱动调用的。
	//
	// 它的第一个参数link为Link接口对象，该对象在master-slave模式下可能是一个主节点对象，也可能是从节点对象，
	// 因此如果在继承的驱动对象实现中使用该link接口对象时，注意当前的运行模式(slave节点在大部分的数据库主从模式中往往是不可写的)。
	DoInsert(link Link, table string, data interface{}, option int, batch ...int) (result sql.Result, err error)
	// Do* 系列方法是给底层驱动调用的。
	//
	// 它的第一个参数link为Link接口对象，该对象在master-slave模式下可能是一个主节点对象，也可能是从节点对象，
	// 因此如果在继承的驱动对象实现中使用该link接口对象时，注意当前的运行模式(slave节点在大部分的数据库主从模式中往往是不可写的)。
	DoBatchInsert(link Link, table string, list interface{}, option int, batch ...int) (result sql.Result, err error)
	// Do* 系列方法是给底层驱动调用的。
	//
	// 它的第一个参数link为Link接口对象，该对象在master-slave模式下可能是一个主节点对象，也可能是从节点对象，
	// 因此如果在继承的驱动对象实现中使用该link接口对象时，注意当前的运行模式(slave节点在大部分的数据库主从模式中往往是不可写的)。
	// Do* 系列方法是给底层驱动调用的。
	//
	// 它的第一个参数link为Link接口对象，该对象在master-slave模式下可能是一个主节点对象，也可能是从节点对象，
	// 因此如果在继承的驱动对象实现中使用该link接口对象时，注意当前的运行模式(slave节点在大部分的数据库主从模式中往往是不可写的)。
	DoUpdate(link Link, table string, data interface{}, condition string, args ...interface{}) (result sql.Result, err error)
	// Do* 系列方法是给底层驱动调用的。
	//
	// 它的第一个参数link为Link接口对象，该对象在master-slave模式下可能是一个主节点对象，也可能是从节点对象，
	// 因此如果在继承的驱动对象实现中使用该link接口对象时，注意当前的运行模式(slave节点在大部分的数据库主从模式中往往是不可写的)。
	DoDelete(link Link, table string, condition string, args ...interface{}) (result sql.Result, err error)

	// 查询并返回多条记录的列表/数组
	GetAll(sql string, args ...interface{}) (Result, error)
	// 查询并返回单条记录
	GetOne(sql string, args ...interface{}) (Record, error)
	// 查询并返回一个字段的值，（往往需要结合Fields方法使用）
	Value(sql string, args ...interface{}) (Value, error)
	// 查询指定字段这一列所有的数据，返回数组。
	Array(sql string, args ...interface{}) ([]Value, error)
	// 查询并返回记录数
	Count(sql string, args ...interface{}) (int, error)
	// 将查询结果转换为一个struct对象。
	//
	// 查询结果应当是特定的一条记录，并且pointer参数应当为struct对象的指针地址（*struct或者**struct）。
	//
	// 例如：
	//
	//  type User struct {
	//     Id         int
	//     Passport   string
	//     Password   string
	//     NickName   string
	//     CreateTime gtime.Time
	// }
	//
	// user := new(User)
	//
	// err  := db.Table("user").Where("id", 1).Struct(user)
	Struct(objPointer interface{}, sql string, args ...interface{}) error
	// 将多条查询结果集转换为一个[]struct/[]*struct数组。
	//
	// 查询结果应当是多条记录组成的结果集，并且pointer应当为数组的指针地址，使用方式例如：
	//
	// users := ([]User)(nil) 或者 var users []User
	//
	// err := db.Table("user").Structs(&users)
	Structs(objPointerSlice interface{}, sql string, args ...interface{}) error
	// 根据输入参数<pointer>的类型自动调用Struct或Structs函数。
	//
	// 如果<pointer>是*Struct/**Struct类型，则调用函数Struct。
	//
	// 如果<pointer>的类型为*[]struct/*[]*struct，则调用函数Structs。
	//
	// 注意: 如果没有检索到任何记录，并且给定的指针不为空或nil，它返回sql.ErrNoRows。
	Scan(objPointer interface{}, sql string, args ...interface{}) error

	// Master 指定操作是在主节点上进行。
	Master() (*sql.DB, error)
	// Slave 指定操作是在从节点上执行。
	//
	// 注意: 只有在配置了任何从属节点时才有意义。
	Slave() (*sql.DB, error)
	// 测试主节点通不通。
	PingMaster() error
	// 测试从节点通不通。
	PingSlave() error

	// 开启事务操作
	Begin() (*TX, error)
	// 事务的闭包操作，输入参数只有一个函数。
	Transaction(f func(tx *TX) error) (err error)

	//
	GetCache() *gcache.Cache
	SetDebug(debug bool)
	GetDebug() bool
	SetSchema(schema string)
	GetSchema() string
	GetPrefix() string
	GetGroup() string
	SetDryRun(dryrun bool)
	GetDryRun() bool
	SetLogger(logger *glog.Logger)
	GetLogger() *glog.Logger
	GetConfig() *ConfigNode
	SetMaxIdleConnCount(n int)
	SetMaxOpenConnCount(n int)
	SetMaxConnLifetime(d time.Duration)

	// 获取上下文操作句柄
	GetCtx() context.Context
	GetChars() (charLeft string, charRight string)
	GetMaster(schema ...string) (*sql.DB, error)
	GetSlave(schema ...string) (*sql.DB, error)
	QuoteWord(s string) string
	QuoteString(s string) string
	// 获取表名前缀的引用。
	QuotePrefixTableName(table string) string
	// 获取数据库中所有的表。
	Tables(schema ...string) (tables []string, err error)
	// 获取数据库中某一张表所有的字段。
	TableFields(table string, schema ...string) (map[string]*TableField, error)
	// 判断数据库中是否有这张表。
	HasTable(name string) (bool, error)
	// 过滤数据库连接信息。
	FilteredLinkInfo() string

	// HandleSqlBeforeCommit 是一个钩子函数，它在将sql字符串提交到底层驱动程序之前处理该字符串。
	//
	// 参数<link>指定当前数据库连接操作对象。
	//
	// 在将sql字符串<sql>及其参数<args>提交到驱动程序之前，可以根据需要修改它们。
	HandleSqlBeforeCommit(link Link, sql string, args []interface{}) (string, []interface{})

	// ===========================================================================
	// 内部方法，对于内部使用目的，您不需要考虑它。
	// ===========================================================================

	mappingAndFilterData(schema, table string, data map[string]interface{}, filter bool) (map[string]interface{}, error)
	convertFieldValueToLocalValue(fieldValue interface{}, fieldType string) interface{}
	convertRowsToResult(rows *sql.Rows) (Result, error)
}

// Core 是数据库管理的基本结构。Core只实现了DB接口一部分方法，剩下没实现的交给 DBDriver (数据库驱动)实现，这样DBDriver只要继承Core，就完全实现了DB接口。
// Core 就是DBDriver 实现DB接口的公共部分，即：父类。
type Core struct {
	DB     DB              // DB 接口对象。(持有这个DB的目的是:保证链式调用的时候调用的是Driver的方法，而不是Core的方法)
	group  string          // 配置组名称。
	debug  *gtype.Bool     // 为数据库启用调试模式，可以在运行时更改。
	cache  *gcache.Cache   // 缓存管理器，仅SQL结果缓存。
	schema *gtype.String   // 此对象的自定义架构。
	logger *glog.Logger    // 日志记录器。
	config *ConfigNode     // 当前配置节点。
	ctx    context.Context // 仅用于链接操作的上下文。
}

// Driver 是将sql驱动程序集成到包gdb中的接口。
type Driver interface {
	// New 为指定的数据库服务器创建并返回数据库对象。
	New(core *Core, node *ConfigNode) (DB, error)
}

// Sql 是sql记录结构体。
type Sql struct {
	Sql    string        // SQL字符串（可能包含保留字符“？”）。
	Type   string        // SQL操作类型。
	Args   []interface{} // 此sql的参数。
	Format string        // 格式化的sql，其中包含sql中的参数。
	Error  error         // 执行结果。
	Start  int64         // Start 执行时间戳（毫秒）。
	End    int64         // End 执行时间戳（毫秒）。
	Group  string        // Group 是从中执行sql的配置的组名。
}

// TableField 是表字段的结构体。
type TableField struct {
	Index   int         // 用于排序，因为（表->结构体）映射是无序的。
	Name    string      // 字段名。映射成结构体的tagKey，例如: `orm:"username"` 字段名就是username
	Type    string      // 字段类型。映射成结构体属性(成员变量)的数据类型。
	Null    bool        // 字段可以为空或不为空。
	Key     string      // 索引信息（如果不是索引，则为空）。
	Default interface{} // 字段的默认值。
	Extra   string      // 额外信息。
	Comment string      // 字段的注释。
}

// Link 是一个通用的数据库函数包装器接口。
//
// Link接口对象在master-slave模式下可能是一个主节点对象，也可能是从节点对象，因此如果在继承的驱动对象实现中使用该link接口对象时，注意当前的运行模式。
// slave节点在大部分的数据库主从模式中往往是不可写的。
type Link interface {
	Query(sql string, args ...interface{}) (*sql.Rows, error)
	Exec(sql string, args ...interface{}) (sql.Result, error)
	Prepare(sql string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error)
	ExecContext(ctx context.Context, sql string, args ...interface{}) (sql.Result, error)
	PrepareContext(ctx context.Context, sql string) (*sql.Stmt, error)
}

// Counter  是更新计数的类型。
type Counter struct {
	Field string
	Value float64
}

type (
	Raw    string                   // Raw 是一个原始sql，它不会被视为参数，而是直接sql部分。
	Value  = *gvar.Var              // Value 是字段值类型。
	Record map[string]Value         // Record 是表的行记录。
	Result []Record                 // Result 是行记录数组。
	Map    = map[string]interface{} // Map 是map[string]interface{}的别名，这是最常用的映射类型。
	List   = []Map                  // List 是map数组的类型。
)

const (
	insertOptionDefault     = 0
	insertOptionReplace     = 1
	insertOptionSave        = 2
	insertOptionIgnore      = 3
	defaultBatchNumber      = 10               // 批量插入/替换/保存的每次计数。
	defaultMaxIdleConnCount = 10               // 池中的最大空闲连接数。
	defaultMaxOpenConnCount = 100              // 池中最大打开连接数。
	defaultMaxConnLifeTime  = 30 * time.Second // 池中每个连接的最长生存时间（秒）。
	ctxTimeoutTypeExec      = iota
	ctxTimeoutTypeQuery
	ctxTimeoutTypePrepare
)

var (
	// ErrNoRows is alias of sql.ErrNoRows.
	ErrNoRows = sql.ErrNoRows

	// instances 是实例的管理映射。
	instances = gmap.NewStrAnyMap(true)

	// driverMap 管理所有自定义注册的驱动程序。
	driverMap = map[string]Driver{
		"mysql":  &DriverMysql{},
		"mssql":  &DriverMssql{},
		"pgsql":  &DriverPgsql{},
		"oracle": &DriverOracle{},
		"sqlite": &DriverSqlite{},
	}

	// lastOperatorRegPattern 是尾部有运算符的字符串的正则表达式模式。
	lastOperatorRegPattern = `[<>=]+\s*$`

	// regularFieldNameRegPattern 字符串的正则表达式模式，该字符串是表的正则字段名。
	regularFieldNameRegPattern = `^[\w\.\-\_]+$`

	// regularFieldNameWithoutDotRegPattern 类似于regularFieldNameRegPattern，但不允许“.”。
	// 请注意，虽然有些数据库允许在字段名中使用字符“.”，但此处不允许在字段名中使用“.”，因为它与数据库表字段“在某些情况下。
	regularFieldNameWithoutDotRegPattern = `^[\w\-\_]+$`

	// internalCache 是供内部使用的内存缓存。
	internalCache = gcache.New()

	// allDryRun 为所有数据库设置 dry-run 功能连接。它为方便起见，通常用于命令选项。
	allDryRun = false
)

func init() {
	// allDryRun 从环境或命令选项初始化。
	allDryRun = gcmd.GetWithEnv("gf.gdb.dryrun", false).Bool()
}

// Register 向gdb注册自定义数据库驱动程序。
func Register(name string, driver Driver) error {
	driverMap[name] = driver
	return nil
}

// New 创建并返回具有全局配置的ORM对象。
// 参数<group>指定配置组名，默认情况下为DefaultGroupName。
func New(group ...string) (db DB, err error) {
	groupName := configs.group
	if len(group) > 0 && group[0] != "" {
		groupName = group[0]
	}
	configs.RLock()
	defer configs.RUnlock()

	if len(configs.config) < 1 {
		return nil, gerror.New("empty database configuration")
	}
	if _, ok := configs.config[groupName]; ok {
		if node, err := getConfigNodeByGroup(groupName, true); err == nil {
			c := &Core{
				group:  groupName,
				debug:  gtype.NewBool(),
				cache:  gcache.New(),
				schema: gtype.NewString(),
				logger: glog.New(),
				config: node,
			}
			if v, ok := driverMap[node.Type]; ok { // 如果注册了数据库驱动
				c.DB, err = v.New(c, node) // 返回一个DB单例
				if err != nil {
					return nil, err
				}
				return c.DB, nil
			} else {
				return nil, gerror.New(fmt.Sprintf(`unsupported database type "%s"`, node.Type))
			}
		} else {
			return nil, err
		}
	} else {
		return nil, gerror.New(fmt.Sprintf(`database configuration node "%s" is not found`, groupName))
	}
}

// Instance returns an instance for DB operations.
// The parameter <name> specifies the configuration group name, which is DefaultGroupName in default.
func Instance(name ...string) (db DB, err error) {
	group := configs.group
	if len(name) > 0 && name[0] != "" {
		group = name[0]
	}
	v := instances.GetOrSetFuncLock(group, func() interface{} {
		db, err = New(group)
		return db
	})
	if v != nil {
		return v.(DB), nil
	}
	return
}

// getConfigNodeByGroup 计算并返回给定组的配置节点。 它使用权重算法在内部计算值以实现负载平衡。
//
// 参数<master>指定是检索主节点，还是从节点（如果已配置主从）。
func getConfigNodeByGroup(group string, master bool) (*ConfigNode, error) {
	if list, ok := configs.config[group]; ok { //根据配置组名group，返回对应的配置组.
		// 分离主配置节点和从配置节点阵列。
		masterList := make(ConfigGroup, 0)
		slaveList := make(ConfigGroup, 0)
		for i := 0; i < len(list); i++ {// list是ConfigGroup类型
			if list[i].Role == "slave" {
				slaveList = append(slaveList, list[i])
			} else {
				masterList = append(masterList, list[i])
			}
		}
		if len(masterList) < 1 {
			return nil, gerror.New("at least one master node configuration's need to make sense")
		}
		if len(slaveList) < 1 {
			slaveList = masterList
		}
		if master {
			return getConfigNodeByWeight(masterList), nil
		} else {
			return getConfigNodeByWeight(slaveList), nil
		}
	} else {
		return nil, gerror.New(fmt.Sprintf("empty database configuration for item name '%s'", group))
	}
}

// getConfigNodeByWeight 计算配置权重并随机返回一个节点。
//
// 计算算法简介:
// 1. 如果我们有2个节点，并且它们的权重均为1，则权重范围为[0，199];
//
// 2. Node1权重范围为[0，99]，node2权重范围为[100，199]，比率为1：1;
//
// 3. 如果随机数是99，则它选择并返回node1;
func getConfigNodeByWeight(cg ConfigGroup) *ConfigNode {
	if len(cg) < 2 {
		return &cg[0]
	}
	var total int
	for i := 0; i < len(cg); i++ {
		total += cg[i].Weight * 100 //ConfigGroup中每一个ConfigNode的权重值 * 100，然后所有ConfigNode的权重值累加
	}
	// 如果total为0，则表示所有节点均未配置权重属性。然后，它将每个节点的weight属性默认设置为1。
	if total == 0 {
		for i := 0; i < len(cg); i++ {
			cg[i].Weight = 1
			total += cg[i].Weight * 100
		}
	}
	// 排除右边框值。
	r := grand.N(0, total-1)
	min := 0
	max := 0
	for i := 0; i < len(cg); i++ {
		max = min + cg[i].Weight*100
		//fmt.Printf("r: %d, min: %d, max: %d\n", r, min, max)
		if r >= min && r < max {
			return &cg[i] // cg[0]即: node1的范围是 [0:99]
		} else {
			min = max
		}
	}
	return nil
}

// getSqlDb 检索并返回一个基础数据库的连接对象,参数<master>指定如果配置了主从节点，则是否检索主节点连接。
func (c *Core) getSqlDb(master bool, schema ...string) (sqlDb *sql.DB, err error) {
	// Load balance.
	node, err := getConfigNodeByGroup(c.group, master)
	if err != nil {
		return nil, err
	}
	// Default value checks.
	if node.Charset == "" {
		node.Charset = "utf8"
	}
	// Changes the schema.
	nodeSchema := c.schema.Val()
	if len(schema) > 0 && schema[0] != "" {
		nodeSchema = schema[0]
	}
	if nodeSchema != "" {
		// Value copy.
		n := *node
		n.Name = nodeSchema
		node = &n
	}
	// 按节点缓存基础连接池对象。
	v, _ := internalCache.GetOrSetFuncLock(node.String(), func() (interface{}, error) {
		sqlDb, err = c.DB.Open(node)
		if err != nil {
			return nil, err
		}
		if c.config.MaxIdleConnCount > 0 {
			sqlDb.SetMaxIdleConns(c.config.MaxIdleConnCount)
		} else {
			sqlDb.SetMaxIdleConns(defaultMaxIdleConnCount)
		}
		if c.config.MaxOpenConnCount > 0 {
			sqlDb.SetMaxOpenConns(c.config.MaxOpenConnCount)
		} else {
			sqlDb.SetMaxOpenConns(defaultMaxOpenConnCount)
		}
		if c.config.MaxConnLifetime > 0 {
			// Automatically checks whether MaxConnLifetime is configured using string like: "30s", "60s", etc.
			// Or else it is configured just using number, which means value in seconds.
			if c.config.MaxConnLifetime > time.Second {
				sqlDb.SetConnMaxLifetime(c.config.MaxConnLifetime)
			} else {
				sqlDb.SetConnMaxLifetime(c.config.MaxConnLifetime * time.Second)
			}
		} else {
			sqlDb.SetConnMaxLifetime(defaultMaxConnLifeTime)
		}
		return sqlDb, nil
	}, 0)
	if v != nil && sqlDb == nil {
		sqlDb = v.(*sql.DB)
	}
	if node.Debug {
		c.DB.SetDebug(node.Debug)
	}
	if node.Debug {
		c.DB.SetDryRun(node.DryRun)
	}
	return
}
