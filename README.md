bumblebee
=============
bumblebee是一个ETL程序，Java语言编写，主要涉及的是抽取（extract）过程，把这个过程抽象为几部分：数据源定义、数据行的解析、转换函数定义及计算、输出文件定义及格式化输出等。

##Key Features:
*定义采用xml文件进行配置，易于读写
*抽象了extract模型，易于扩展
*支持多种计算模式
	*map/reduce：把定义xml文件解析成extract模型，并把模型的java类序列化为json字符串，在mapper/reducer中反序列化为java类，进行相关计算
	*localFile：在本地进行数据源文件的转换，每个文件一个线程，每次

##Adventage

##Getting started
*运行环境：jdk1.7+
*release version,download
*usage
**
*支持功能


##Performance 


##Contributors 
