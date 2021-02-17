// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import "database/sql"

// SqlResult 是sql操作的执行结果。它还支持rowsAffected的批处理操作结果。
type SqlResult struct {
	result   sql.Result
	affected int64
}

// MustGetAffected 返回受影响的行数，如果发生任何错误，它将崩溃。
func (r *SqlResult) MustGetAffected() int64 {
	rows, err := r.RowsAffected()
	if err != nil {
		panic(err)
	}
	return rows
}

// MustGetInsertId 返回最后一个insert id，如果发生任何错误，它将崩溃。
func (r *SqlResult) MustGetInsertId() int64 {
	id, err := r.LastInsertId()
	if err != nil {
		panic(err)
	}
	return id
}

// see sql.Result.RowsAffected
func (r *SqlResult) RowsAffected() (int64, error) {
	if r.affected > 0 {
		return r.affected, nil
	}
	if r.result == nil {
		return 0, nil
	}
	return r.result.RowsAffected()
}

// see sql.Result.LastInsertId
func (r *SqlResult) LastInsertId() (int64, error) {
	if r.result == nil {
		return 0, nil
	}
	return r.result.LastInsertId()
}
