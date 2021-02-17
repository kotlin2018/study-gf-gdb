// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import (
	"time"
)

// Cache 设置model的缓存功能。它缓存sql的结果，这意味着如果有另一个相同的sql请求，它只是从缓存中读取并返回结果，而不是提交并执行到数据库中。
//
// 如果参数<duration><0，这意味着它用给定的<name>清除缓存。
//
// 如果参数<duration>=0，则表示它永不过期。
//
// 如果参数<duration>>0，则表示它在<duration>之后过期。
//
// 可选参数<name>用于将名称绑定到缓存，这意味着您可以稍后控制缓存，如更改<duration>或使用指定的<name>清除缓存。
//
// 请注意，如果模型正在事务上执行select语句，则缓存功能将被禁用。
func (m *Model) Cache(duration time.Duration, name ...string) *Model {
	model := m.getModel()
	model.cacheDuration = duration
	if len(name) > 0 {
		model.cacheName = name[0]
	}
	model.cacheEnabled = true
	return model
}

// checkAndRemoveCache 如果启用了缓存功能，则检查并删除insert/update/delete语句中的缓存。
func (m *Model) checkAndRemoveCache() {
	if m.cacheEnabled && m.cacheDuration < 0 && len(m.cacheName) > 0 {
		m.db.GetCache().Ctx(m.db.GetCtx()).Remove(m.cacheName)
	}
}
