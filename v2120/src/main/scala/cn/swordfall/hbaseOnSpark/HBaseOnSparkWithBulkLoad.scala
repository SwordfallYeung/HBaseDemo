package cn.swordfall.hbaseOnSpark

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
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
    * 批量插入数据 只有一列
    */
  def insertWithBulkLoadWithKeyValue(): Unit ={
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
    rdd.saveAsNewAPIHadoopFile("hdfs://node1:9000/test", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    bulkLoader.doBulkLoad(new Path("hdfs://node1:9000/test"), admin, table, conn.getRegionLocator(TableName.valueOf(tableName)))
  }

  /**
    * 批量插入 多列 Put
    */
  def insertWithBulkLoadWithPut(): Unit ={

    val inputPath = "v2120/a.txt"
    val outputPath = "hdfs://node1:9000/test"

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
    job.setJarByClass(classOf[HBaseOnSparkWithBulkLoad])
    job.setMapperClass(classOf[GenerateHFileScala])
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[HFileOutputFormat2])
    HFileOutputFormat2.configureIncrementalLoad(job, table, conn.getRegionLocator(TableName.valueOf(tableName)))

    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath))
    //job.waitForCompletion(true)

    if (job.waitForCompletion(true)){
      val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
      bulkLoader.doBulkLoad(new Path(outputPath), admin, table, conn.getRegionLocator(TableName.valueOf(tableName)))
    }
  }
}
