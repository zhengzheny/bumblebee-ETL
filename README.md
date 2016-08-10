bumblebee
=============
bumblebee是一个ETL程序，Java语言编写，主要涉及的是抽取（extract）过程，把这个过程抽象为几部分：数据源定义、数据行的解析、转换函数定义及计算、输出文件定义及格式化输出等。

##Key Features
* 定义采用xml文件进行配置，易于读写
* 抽象了extract模型，易于扩展
* 支持多种计算模式：
	* map/reduce：把定义xml文件解析成extract模型，并把模型的java类序列化为json字符串，在mapper/reducer中反序列化为java类，进行相关计算。
	* localFile：在本地进行数据源文件的转换，每个文件一个线程，文件通过java nio进行内存映射，每次读取1M（可设置）内容，解析成一行行由handler类处理。
	* sliceLocalFile:处理方法类似于localFile，但针对同一个文件，又分成了多个线程进行处理。这种模式主要处理每一行都是独立的内容块，线程之间可以方便拆分，而localFile处理如数据源为xml的内容块，不同线程拆间拆分比较麻烦，故在同一个线程中处理。
	* spark streaming：在streaming中对数据进行转换和处理。
	* flume：利用flume的interceptors对数据进行处理。重构了SpoolDirectorySource代码，支持.gz压缩格式。

##Adventage
* 可以快速扩展数据源，数据源可以是单个文件，也可以是目录，目前已经支持的数据源：
	* simpleFlat：简单分隔符的文本文件，需要详细定义每一个字段
	* noSettingFieldsFlat：无需定义数据源字段，框架自动把数据行，按照分割符解析成“_n”字段，意味着第n个字段。
	* fixedLengthFlat：为字段长度固定类型，每一个字段长度定义好是固定的，在field的length属性中定义长度。
	* fixedLengthByLineFlat:也是字段固定长度类型，但是根据数据源中的某一行自行判断每一个字段的长度。
* 可以快速扩展过滤器和转换函数
	* 过滤器：在转换函数之前，通过过滤条件才可以进行函数转换
	* 转换函数：支持输出中间结果，支持表示所有字段的通配符"*"
	* 维表查询:
		* 来源：主要形式为Key-Value，用来查询和转换数据源中相关字段的值，来源可以是在xml配置中自定义、本地文件、HDFS和数据库。
		* 查询：支持三种查询，维表是否存在，根据key查询value，根据value反查key。
* 输出格式的快速定义
* 字段校验：
	* 类型校验:支持的类型包含byte, creditCard, double,email, float, int, long, short, string, idCard,其中creditCard为信用卡号，idCard为身份证号校验是否合法。
	* 字段长度校验:设置字段minLength和maxLength属性，字段长度小于minLength；或者字段长度大于maxLength，都会报错。
	* 字段是否为空:默认为可以为空，如果需要设置不为空，设置值为yes。
	* 是否严格检查:设置field属性strictCheck，默认为不严格检查，如果需要严格检查，设置值为yes。如果出现字段校验异常，严格检查的话，框架将写校验异常文件，将不会继续做函数转换。如果非严格检查（即默认情况），框架会写校验异常文件，并继续进行函数转换，进而输出到文件中。非严格检查的目的，考虑些字段需要检验出来，但这个数据还是可以用的，可以做其他处理的，因为客户等着用数，需要进入下一环节处理。如果直接出错不做后续的转换，那么数据将跑不下去，必须追数据源把数据校正了才行，这样会影响客户用数。
* 错误记录处理：
	* 会把字段校验和函数转换时候的错误写入到无效数据和错误文件中。
	* 针对map/reduce的错误记录，在控制台无法看到，不利于运维，框架会记录每个mapper中的错误，并在控制台进行集中打印。
* 变量引用：
	* .properties文件定义：在xml配置中，有些属性值希望通过变量方式引入，如路径属性：/data/etl/ /TB_CUST_INFO_M/YYYYMM/，YYYYMM每个月都要变化；另外有些属性非常长，放在xml中可读性差；还有一些经常变化的属性，希望像spring一样集中在某一个*. properties文件中进行管理，这样每次修改，只需要修改.properties文件。
	* shell命令行的变量应用：YYYYMM可以通过shell命令行传递参数进去，用法如下：bin/etl.sh configFile=conf/test/etl.xml  processId=ex1 YYYYMM=201505
		
##Getting started
* 运行环境：jdk1.7+
* release version:![download](https://github.com/styg/bumblebee-ETL/releases)
* 编译及部署：
	* 下载 ![Maven](http://maven.apache.org/) ，并配置执行路径
	* 在源代码下执行mvn package，会在target目录下产生etl-1.0-linux.tar.gz文件，在linux下解压即可
* usage：
	* bin/etl.sh configFile=conf/test/etl.xml  processId=ex1 YYYYMM=201505
	* 其中processId为执行的业务流程，YYYYMM为变量引用举例

##Performance 
* map/reduce性能取决于hadoop集群的性能
* slice local file计算模式，可以达到124M/s处理速度
	* cpu：Intel(R) Xeon(R) CPU E5-2630 v3 @ 2.40GHz ，8个cpu，每个cpu4核，共计32core,132G内存
	* 56.33 GB文件，记录数5.6亿，做分割符转换，耗时465秒
*Flume取决于agent性能 

##Contributors 
* 田熙清  531013850@qq.com
* 韩二明  283741965@qq.com
