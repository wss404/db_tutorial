序列化就是将高级语言的数据及数据结构按照一定形式组织以持久化到硬盘或者进行传输

这个变化过程可逆，它的逆过程即反序列化，从硬盘或其它进程读取到序列化数据之后，将这些数据转换成本进程可以直接使用的数据

json或xml格式的数据可被各种语言使用

Gob是一种以Go自己的二进制格式序列化和反序列化程序数据的格式

protobuff序列化

boltdb的处理则更为简单粗暴
![boltdb](./img.png)