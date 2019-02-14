package cn.swordfall.hbaseOnSpark

import java.net.URI

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * @Author: Yang JianQiu
  * @Date: 2019/1/31 18:07
  *   批量插入
  *  参考资料：
  *  https://www.cnblogs.com/simple-focus/p/6879971.html
  *  https://www.cnblogs.com/MOBIN/p/5559575.html
  *  https://blog.csdn.net/Suubyy/article/details/80892023
  *  https://www.jianshu.com/p/b09283b14d84
  */
class HBaseOnSparkWithBulkLoad {

  /**
    * 普通插入
    */
  def commonInsert(): Unit ={
    val spark = SparkSession.builder().appName("commonInsert").master("local[4]").getOrCreate()

    val rdd = spark.sparkContext.textFile("v2120/a.txt")
    val data = rdd.map(_.split(",")).map{x => (x(0), x(1), x(2))}
    val result = data.foreachPartition{partition => {
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.187.201")
      hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
      hbaseConf.set(TableInputFormat.INPUT_TABLE, "test")
      //hbaseConf.addResource("/home/hadoop/data/lib/hbase-site.xml")

      val conn = ConnectionFactory.createConnection(hbaseConf)
      val table = conn.getTable(TableName.valueOf("test"))

      partition.foreach{ row =>
        val put = new Put(Bytes.toBytes(row._1))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes(row._2))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(row._3))
        table.put(put)
      }
      table.close()
    }}
  }

  /**
    * 批量插入数据 单列
    */
  def insertWithBulkLoadWithSingle(): Unit ={
    val sparkSession = SparkSession.builder().appName("insertWithBulkLoad").master("local[4]").getOrCreate()
    val sc = sparkSession.sparkContext

    val tableName = "test"
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.187.201")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val admin = conn.getAdmin
    val table = conn.getTable(TableName.valueOf(tableName))

    lazy val job = Job.getInstance(hbaseConf)
    //设置job的输出格式
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    //只有一列用KeyValue，多列用Put
    job.setMapOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat2])
    HFileOutputFormat2.configureIncrementalLoad(job, table, conn.getRegionLocator(TableName.valueOf(tableName)))

    val rdd = sc.textFile("v2120/a.txt")
      .map(_.split(","))
      .map{x => (DigestUtils.md5Hex(x(0)).substring(0, 3) + x(0) , x(1), x(2))}
      .sortBy(x => x._1)
      .map{x =>
        {
          val kv1: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes(x._2 + ""))
          (new ImmutableBytesWritable(), kv1)
        }
      }

    isFileExist("hdfs://node1:9000/test", sc)

    rdd.saveAsNewAPIHadoopFile("hdfs://node1:9000/test", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    bulkLoader.doBulkLoad(new Path("hdfs://node1:9000/test"), admin, table, conn.getRegionLocator(TableName.valueOf(tableName)))
  }

  /**
    * 批量插入 多列
    */
  def insertWithBulkLoadWithMulti(): Unit ={

    val sparkSession = SparkSession.builder().appName("insertWithBulkLoad").master("local[4]").getOrCreate()
    val sc = sparkSession.sparkContext

    val tableName = "test"
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

    val rdd = sc.textFile("v2120/a.txt")
      .map(_.split(","))
      .map(x => (DigestUtils.md5Hex(x(0)).substring(0, 3) + x(0), x(1), x(2)))
      .sortBy(_._1)
      .flatMap(x =>
        {
          val listBuffer = new ListBuffer[(ImmutableBytesWritable, KeyValue)]
          val kv1: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes(x._2 + ""))
          val kv2: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(x._3 + ""))
          listBuffer.append((new ImmutableBytesWritable, kv2))
          listBuffer.append((new ImmutableBytesWritable, kv1))
          listBuffer
        }
      )
    //多列的排序，要按照列名字母表大小来

    isFileExist("hdfs://node1:9000/test", sc)

    rdd.saveAsNewAPIHadoopFile("hdfs://node1:9000/test", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    bulkLoader.doBulkLoad(new Path("hdfs://node1:9000/test"), admin, table, conn.getRegionLocator(TableName.valueOf(tableName)))
  }

  def isFileExist(filePath: String, sc: SparkContext): Unit ={
    /*
    hadoop写法
    val hdfs = FileSystem.get(new URI(filePath), new Configuration)
    if (hdfs.exists(new Path(filePath))){
      hdfs.delete(new Path(filePath), true)
    }*/

    /*spark 写法*/
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConf)
    if (hdfs.exists(new Path(filePath))){
      hdfs.delete(new Path(filePath), true)
    }

  }
}
