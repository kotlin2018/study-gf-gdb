// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

// Option 为Model添加额外的操作选项。
func (m *Model) Option(option int) *Model {
	model := m.getModel()
	model.option = model.option | option
	return model
}

//// OptionOmitEmpty sets OPTION_OMITEMPTY option for the model, which automatically filers
//// the data and where attributes for empty values.
//// Deprecated, use OmitEmpty instead.
//func (m *Model) OptionOmitEmpty() *Model {
//	return m.Option(OPTION_OMITEMPTY)
//}


// OmitEmpty 空值过滤，(过滤输入参数中的空值: nil,"",0)。
func (m *Model) OmitEmpty() *Model {
	return m.Option(OptionOmitempty)
}
