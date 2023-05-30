### 项目起源
在学习数据库原理的过程中发现一个[用C实现简单数据库的项目](https://cstack.github.io/db_tutorial/)，它同[boltdb](https://github.com/boltdb/bolt)的实现有相似之处。

### 开发步骤
1. 实现交互式编程环境；
2. 实现简单sql编译器和虚拟机；
3. 在内存实现仅支持添加、单表的数据库；
4. 测试方案；
5. 持久化到硬盘；
6. 抽象出游标；
7. B树叶子节点的格式；
8. 实现二分查找；
9. 分裂叶子节点；
10. 实现递归查找；
11. 扫描一棵多层的B树；
12. 分裂节点后更新父节点。

### Tips
序列化（serializeRow）、反序列化（deSerializeRow）函数以及移动节点cell的函数（moveTo）借鉴自boltdb项目。
