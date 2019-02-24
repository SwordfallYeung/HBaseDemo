# HBaseDemo
Hbase的基本使用及基本demo

$HBASE_HOME/bin/hbase shell<br/>
报错：Error: Could not find or load main class org.jruby.Main
原因：
参考资料：https://stackoverflow.com/questions/25479378/could-not-find-or-load-main-class-org-jruby-main-when-run-hbase-shell<br/>

HBase单机版搭建参考：<br/>
https://www.cnblogs.com/xuwujing/p/8017116.html<br/>
https://blog.csdn.net/qazwsxpcm/article/details/78637874<br/>

写入到HBase的方式有：
1) 调用HBase API，使用Table.put方式单条写入
2) MapReduce方式，使用TableOutputFormat作为输出
3) Bulk Load方式，先将要推入的数据按照格式持久化成HFile文件，然后使用HBase对该文件进行load

https://segmentfault.com/a/1190000009762041

### Mongo、Redis、HBase主流nosql性能对比
参考资料：https://bbs.huaweicloud.com/blogs/7c0b2684201d11e7b8317ca23e93a891
Mongodb读性能优于写性能，比较像Mysql；HBase写性能优于读性能。

### 什么场景下使用HBase、Cassandra
如果有几亿或者几十亿条记录要出入HBase，那么HBase就是一个正确的选择；否则如果你仅有几百万条甚至更少的数据，那么HBase当然不是正确的选择，这种情况下应该选择传统的关系型数据库。

### 大数据的数据库混合使用模式
参考资料：http://www.h3c.com/cn/d_201511/901094_30008_0.htm

### 大数据平台中关系型数据库、DB2、HBase分别做什么
关系型数据库是小型前台报表库，存放大数据加工好的数据，前台展示用。DB2则是历史原因，用于集团接口数据库，后期将逐步演进为分布式架构，HBase主要用于高并发查询，如日志查询，它的数据来源于ETL加工处理好的明细数据。

### HBase数据库rowKey及二级索引
rowkey设计：<br/>
常用的rowkey设计为：salt (1 byte) + attr1_id (4 bytes) + timestamp (4 bytes) + attr2_id (4 bytes) <br/>
https://blog.csdn.net/chengyuqiang/article/details/79134549

rowkey避免热点：<br/>
https://my.oschina.net/lanzp/blog/477732

HBase二级索引：<br/>
https://blog.csdn.net/WYpersist/article/details/79830811<br/>
https://www.jianshu.com/p/0ccd187910e5

### HBase Phoenix 二级索引设计
https://segmentfault.com/a/1190000016311988<br/>
https://www.cnblogs.com/haoxinyue/p/6832964.html<br/>

### HBase性能优化
一般包括软件层面优化和系统层面优化：<br/>
