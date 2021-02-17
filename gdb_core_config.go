// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import (
	"fmt"
	"github.com/gogf/gf/os/gcache"
	"sync"
	"time"

	"github.com/gogf/gf/os/glog"
)

const (
	DefaultGroupName = "default" // Default group name.
)

// Config 是配置管理对象。
type Config map[string]ConfigGroup

// ConfigGroup 是指定命名组的配置节点切片。
type ConfigGroup []ConfigNode

// ConfigNode 是一个节点的配置。
type ConfigNode struct {
	Host                 string        `json:"host"`                 // 服务器、ip或域的主机，如：127.0.0.1，localhost
	Port                 string        `json:"port"`                 // Port, 一般是3306。
	User                 string        `json:"user"`                 // 身份验证用户名。
	Pass                 string        `json:"pass"`                 // 身份验证密码。
	Name                 string        `json:"name"`                 // Default used database name.
	Type                 string        `json:"type"`                 // 数据库类型：mysql、sqlite、mssql、pgsql、oracle。
	Role                 string        `json:"role"`                 // (可选，默认为“主”)节点角色，用于主从模式：主、从。
	Debug                bool          `json:"debug"`                // (Optional) 调试模式启用调试信息记录和输出。
	Prefix               string        `json:"prefix"`               // (Optional) 表前缀。
	DryRun               bool          `json:"dryRun"`               // (Optional) Dry run，不选择INSERT/UPDATE/DELETE语句。
	Weight               int           `json:"weight"`               // (Optional) 用于负载平衡计算的权重，如果只有一个节点就没有用了。
	Charset              string        `json:"charset"`              // (Optional, "utf8mb4" in default) 在数据库上操作时的自定义字符集。
	LinkInfo             string        `json:"link"`                 // (Optional) 使用自定义链接信息时，配置主机/Port/User/Pass/Name将被忽略。
	MaxIdleConnCount     int           `json:"maxIdle"`              // (Optional) 基础连接池的最大空闲连接配置。
	MaxOpenConnCount     int           `json:"maxOpen"`              // (Optional) 基础连接池的最大打开连接配置。
	MaxConnLifetime      time.Duration `json:"maxLifetime"`          // (Optional) 基础连接池的最大连接TTL配置。
	QueryTimeout         time.Duration `json:"queryTimeout"`         // (Optional) 每个dql的最大查询时间。
	ExecTimeout          time.Duration `json:"execTimeout"`          // (Optional) dml的最长执行时间。
	TranTimeout          time.Duration `json:"tranTimeout"`          // (Optional) 事务的最大执行时间。
	PrepareTimeout       time.Duration `json:"prepareTimeout"`       // (Optional) 预加载操作的最大执行时间。
	CreatedAt            string        `json:"createdAt"`            // (Optional) 用于自动填充创建日期时间的表的文件名。
	UpdatedAt            string        `json:"updatedAt"`            // (Optional) 用于自动填充更新日期时间的表的文件名。
	DeletedAt            string        `json:"deletedAt"`            // (Optional) 用于自动填充更新日期时间的表的文件名。
	TimeMaintainDisabled bool          `json:"timeMaintainDisabled"` // (Optional) 禁用自动计时功能。
}

// configs 是内部使用的配置对象。
var configs struct {
	sync.RWMutex
	config Config // 所有配置。
	group  string // 默认配置组。
}

func init() {
	configs.config = make(Config)
	configs.group = DefaultGroupName
}

// SetConfig 设置包的全局配置。它将覆盖包的旧配置。
func SetConfig(config Config) {
	defer instances.Clear()
	configs.Lock()
	defer configs.Unlock()
	configs.config = config
}

// SetConfigGroup 设置给定组的配置。
func SetConfigGroup(group string, nodes ConfigGroup) {
	defer instances.Clear()
	configs.Lock()
	defer configs.Unlock()
	configs.config[group] = nodes
}

// AddConfigNode 将一个节点配置添加到给定组的配置中。
func AddConfigNode(group string, node ConfigNode) {
	defer instances.Clear()
	configs.Lock()
	defer configs.Unlock()
	configs.config[group] = append(configs.config[group], node)
}

// AddDefaultConfigNode 将一个节点配置添加到默认组的配置中。
func AddDefaultConfigNode(node ConfigNode) {
	AddConfigNode(DefaultGroupName, node)
}

// AddDefaultConfigGroup 将多个节点配置添加到默认组的配置中。
func AddDefaultConfigGroup(nodes ConfigGroup) {
	SetConfigGroup(DefaultGroupName, nodes)
}

// GetConfig 检索并返回给定组的配置。
func GetConfig(group string) ConfigGroup {
	configs.RLock()
	defer configs.RUnlock()
	return configs.config[group]
}

// SetDefaultGroup 设置默认配置的组名。
func SetDefaultGroup(name string) {
	defer instances.Clear()
	configs.Lock()
	defer configs.Unlock()
	configs.group = name
}

// GetDefaultGroup 返回默认配置的名称。
func GetDefaultGroup() string {
	defer instances.Clear()
	configs.RLock()
	defer configs.RUnlock()
	return configs.group
}

// IsConfigured 检查并返回数据库是否已配置。如果存在任何配置，则返回true。
func IsConfigured() bool {
	configs.RLock()
	defer configs.RUnlock()
	return len(configs.config) > 0
}

// SetLogger 设置orm的日志记录器。
func (c *Core) SetLogger(logger *glog.Logger) {
	c.logger = logger
}

// GetLogger returns the logger of the orm.
func (c *Core) GetLogger() *glog.Logger {
	return c.logger
}

// SetMaxIdleConnCount sets the max idle connection count for underlying connection pool.
func (c *Core) SetMaxIdleConnCount(n int) {
	c.config.MaxIdleConnCount = n
}

// SetMaxOpenConnCount sets the max open connection count for underlying connection pool.
func (c *Core) SetMaxOpenConnCount(n int) {
	c.config.MaxOpenConnCount = n
}

// SetMaxConnLifetime sets the connection TTL for underlying connection pool.
// If parameter <d> <= 0, it means the connection never expires.
func (c *Core) SetMaxConnLifetime(d time.Duration) {
	c.config.MaxConnLifetime = d
}

// String returns the node as string.
func (node *ConfigNode) String() string {
	return fmt.Sprintf(
		`%s@%s:%s,%s,%s,%s,%s,%v,%d-%d-%d#%s`,
		node.User, node.Host, node.Port,
		node.Name, node.Type, node.Role, node.Charset, node.Debug,
		node.MaxIdleConnCount,
		node.MaxOpenConnCount,
		node.MaxConnLifetime,
		node.LinkInfo,
	)
}

// GetConfig returns the current used node configuration.
func (c *Core) GetConfig() *ConfigNode {
	return c.config
}

// SetDebug enables/disables the debug mode.
func (c *Core) SetDebug(debug bool) {
	c.debug.Set(debug)
}

// GetDebug returns the debug value.
func (c *Core) GetDebug() bool {
	return c.debug.Val()
}

// GetCache returns the internal cache object.
func (c *Core) GetCache() *gcache.Cache {
	return c.cache
}

// GetGroup returns the group string configured.
func (c *Core) GetGroup() string {
	return c.group
}

// SetDryRun enables/disables the DryRun feature.
// Deprecated, use GetConfig instead.
func (c *Core) SetDryRun(enabled bool) {
	c.config.DryRun = enabled
}

// GetDryRun returns the DryRun value.
// Deprecated, use GetConfig instead.
func (c *Core) GetDryRun() bool {
	return c.config.DryRun || allDryRun
}

// GetPrefix returns the table prefix string configured.
// Deprecated, use GetConfig instead.
func (c *Core) GetPrefix() string {
	return c.config.Prefix
}

// SetSchema changes the schema for this database connection object.
// Importantly note that when schema configuration changed for the database,
// it affects all operations on the database object in the future.
func (c *Core) SetSchema(schema string) {
	c.schema.Set(schema)
}

// GetSchema returns the schema configured.
func (c *Core) GetSchema() string {
	return c.schema.Val()
}
