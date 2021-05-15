package engine_util

/**
	引擎是用于在本地存储键值对的低级系统（没有分发或任何事务支持等）.
	该软件包包含用于与此类引擎进行交互的代码。 CF的意思是“列家庭”.
	 列族描述参考： https：//github.comfacebookrocksdbwikiColumn-Families（专门针对RocksDB，但一般概念是通用的）.
	简而言之，列族是关键名称空间.
	通常将多个列族实现为几乎独立的数据库.
	可以使跨列族的写入成为原子的，而对于单独的数据库则无法做到
	engine_util包括以下软件包：
		engines：一种数据结构，用于保持unistore所需的引擎
		write_batch：用于批量写入单个原子“事务”的代码
		cf_iterator：在badge中遍历整个列族的代码
 */
