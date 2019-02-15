package cn.swordfall.hbaseOnSpark

import java.net.URI
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableOutputFormat}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * @Author: Yang JianQiu
  * @Date: 2019/1/31 2:11
  * 参考资料：
  * https://www.cnblogs.com/wumingcong/p/6044038.html
  * https://blog.csdn.net/zhuyu_deng/article/details/43192271
  * https://www.jianshu.com/p/4c908e419b60
  * https://blog.csdn.net/Colton_Null/article/details/83387995
  */
class HBaseOnSparkWithPhoenix {

  def readFromHBaseWithPhoenix: Unit ={
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkSession = SparkSession.builder().appName("SparkHBaseDataFrame").master("local[4]").getOrCreate()

    //表小写，需要加双引号，否则报错
    val dbTable = "\"test\""

    //spark 读取 phoenix 返回 DataFrame的第一种方式
    val rdf = sparkSession.read
      .format("jdbc")
      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
      .option("url", "jdbc:phoenix:192.168.187.201:2181")
      .option("dbtable", dbTable)
      .load()

    val rdfList = rdf.collect()
    for (i <- rdfList){
      println(i.getString(0) + " " + i.getString(1) + " " + i.getString(2))
    }
    rdf.printSchema()

    //spark 读取 phoenix 返回 DataFrame的第二种方式
    val df = sparkSession.read
      .format("org.apache.phoenix.spark")
      .options(Map("table" -> dbTable, "zkUrl" -> "192.168.187.201:2181"))
      .load()
    df.printSchema()
    val dfList = df.collect()
     for (i <- dfList){
       println(i.getString(0) + " " + i.getString(1) + " " + i.getString(2))
     }
   //spark DataFrame 写入 phoenix，需要先建好表
   /*df.write
     .format("org.apache.phoenix.spark")
     .mode(SaveMode.Overwrite)
     .options(Map("table" -> "PHOENIXTESTCOPY", "zkUrl" -> "jdbc:phoenix:192.168.187.201:2181"))
     .save()
*/
   sparkSession.stop()
 }

  /**
    * 测试demo ,使用搜狗实验室的数据http://www.sogou.com/labs/resource/q.php
    * 批量插入 多列 测试phoenix的二级索引导入 1,724,264条数据的
    * 二级索引覆盖索引
    * https://www.cnblogs.com/MOBIN/p/5467284.html
    * https://blog.csdn.net/sinat_36121406/article/details/82839003
    * https://www.jianshu.com/p/a3c24638b498
    */
  def insertWithBulkLoadWithMultiTestDemo(): Unit ={

    val sparkSession = SparkSession.builder().appName("insertWithBulkLoad").master("local[4]").getOrCreate()
    val sc = sparkSession.sparkContext

    val tableName = "log"
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.187.201")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val conn = ConnectionFactory.createConnection(hbaseConf)
    val admin = conn.getAdmin
    val table = conn.getTable(TableName.valueOf(tableName))

    val job = Job.getInstance(hbaseConf)
    //设置job的输出格式
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat2])
    HFileOutputFormat2.configureIncrementalLoad(job, table, conn.getRegionLocator(TableName.valueOf(tableName)))

    val rdd = sc.textFile("v2120/SogouQ.reduced")
      .map(_.split("\t"))
      .map(x => (UUID.randomUUID().toString, x(0), x(1), x(2), x(3), x(4)))
      .sortBy(_._1)
      .flatMap(x =>
      {
        val listBuffer = new ListBuffer[(ImmutableBytesWritable, KeyValue)]
        val kv1: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes("cf1"), Bytes.toBytes("time"), Bytes.toBytes(x._2 + ""))
        val kv2: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes("cf1"), Bytes.toBytes("userId"), Bytes.toBytes(x._3 + ""))
        val kv3: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes("cf1"), Bytes.toBytes("keyWord"), Bytes.toBytes(x._4 + ""))
        val kv4: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes("cf1"), Bytes.toBytes("ranking"), Bytes.toBytes(x._5 + ""))
        val kv6: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes("cf1"), Bytes.toBytes("url"), Bytes.toBytes(x._6 + ""))
        listBuffer.append((new ImmutableBytesWritable, kv3))
        listBuffer.append((new ImmutableBytesWritable, kv4))
        listBuffer.append((new ImmutableBytesWritable, kv1))
        listBuffer.append((new ImmutableBytesWritable, kv6))
        listBuffer.append((new ImmutableBytesWritable, kv2))
        listBuffer
      }
      )
    //多列的排序，要按照列名字母表大小来

    isFileExist("hdfs://node1:9000/log", sc)

    rdd.saveAsNewAPIHadoopFile("hdfs://node1:9000/log", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    bulkLoader.doBulkLoad(new Path("hdfs://node1:9000/log"), admin, table, conn.getRegionLocator(TableName.valueOf(tableName)))
  }

  def isFileExist(filePath: String, sc: SparkContext): Unit ={
    val output = new Path(filePath)
    val hdfs = FileSystem.get(new URI(filePath), new Configuration)
    if (hdfs.exists(output)){
      hdfs.delete(output, true)
    }
  }
}
