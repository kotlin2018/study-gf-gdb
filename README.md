
* `core` 结构体内嵌套了 `DB`接口，也就是说 `core` 是 `DB` 的父类，`DB`是 `core`的成员；


* 因此`core`就包含了 `DB` 接口所有的函数，所以 `core` 并不需要完全实现 `DB`中所有函数，实现一部分即可。


* 同理，新注册的数据库驱动，如果需要选择性的实现 `DB`接口中的一些函数，只需内嵌 `core` 结构体即可。


* 介于这种依赖关系，即使: `core`实现了`DB`一部分函数A，数据库驱动实现了`core`剩下未实现的函数B，
  所以数据库驱动通过`core`这个桥梁完全实现了`DB`接口。