package cn.swordfall.hbaseOnSpark

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

/**
  * @Author: Yang JianQiu
  * @Date: 2019/1/31 18:07
  *   批量插入
  *  参考资料：
  *  https://www.cnblogs.com/simple-focus/p/6879971.html
  *  https://www.cnblogs.com/MOBIN/p/5559575.html
  */
class HBaseOnSparkWithBulkLoad {

  /**
    * 普通插入
    */
  def commonInsert(): Unit ={
    val spark = SparkSession.builder().appName("commonInsert").master("local[4]").getOrCreate()

    val rdd = spark.sparkContext.textFile("/data/produce/2018-03-01.log")
    val data = rdd.map(_.split(",")).map{x => (x(0) + x(1), x(2))}
    val result = data.foreachPartition{partition => {
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "localhost")
      hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
      hbaseConf.set(TableInputFormat.INPUT_TABLE, "data")
      hbaseConf.addResource("/home/hadoop/data/lib/hbase-site.xml")

      val conn = ConnectionFactory.createConnection(hbaseConf)
      val table = conn.getTable(TableName.valueOf("data"))

      partition.foreach{ row =>
        val put = new Put(Bytes.toBytes(row._1))
        put.addColumn(Bytes.toBytes("v"), Bytes.toBytes("value"), Bytes.toBytes(row._2))
        table.put(put)
      }
      table.close()
    }}
  }

  /**
    * 批量插入数据
    */
  def insertWithBulkLoad(): Unit ={
    val sparkSession = SparkSession.builder().appName("insertWithBulkLoad").master("local[4]").getOrCreate()
    val sc = sparkSession.sparkContext

    val tableName = "data1"
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "localhost")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val admin = conn.getAdmin
    val table = conn.getTable(TableName.valueOf(tableName))

    lazy val job = Job.getInstance(hbaseConf)
    //设置job的输出格式
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])

    val rdd = sc.textFile("/data/produce/2015/2015-03-01.log")
      .map(_.split("@"))
      .map{x => (DigestUtils.md5Hex(x(0) + x(1)).substring(0, 3) + x(0) + x(1), x(2))}
      .sortBy(x => x._1)
      .map{x =>
        {
          val kv: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes("v"), Bytes.toBytes("value"), Bytes.toBytes(x._2 + ""))
          (new ImmutableBytesWritable(kv.getKey), kv)
        }
      }
    rdd.saveAsNewAPIHadoopFile("/tmp/data1", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    bulkLoader.doBulkLoad(new Path("/tmp/data1"), admin, table, conn.getRegionLocator(TableName.valueOf(tableName)))
  }
}
