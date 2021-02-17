// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

// LockUpdate 用于创建for update锁，避免选择行被其它共享锁修改或删除，for update会阻塞其他锁定性读对锁定行的读取。
//
// 例如:
//
// db.Table("users").Where("votes>?", 100).LockUpdate().All() 等价于:
// SELECT * FROM 'users'' WHERE 'votes' > 100 FOR UPDATE
func (m *Model) LockUpdate() *Model {
	model := m.getModel()
	model.lockInfo = "FOR UPDATE"
	return model
}

// LockShared 使用LockShared方法，在运行sql语句时带一把”共享锁“，共享锁可以避免被选择的行被修改，直到事务提交。
//
// 例如:
//
// db.Table("users").Where("votes>?", 100).LockShared().All() 等价于:
// SELECT * FROM 'users'' WHERE 'votes' > 100 LOCK IN SHARE MODE
func (m *Model) LockShared() *Model {
	model := m.getModel()
	model.lockInfo = "LOCK IN SHARE MODE"
	return model
}
