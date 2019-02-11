package cn.swordfall.hbaseOnSpark

import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.sql.SparkSession

/**
  * @Author: Yang JianQiu
  * @Date: 2019/1/31 18:07
  *  参考资料：https://www.cnblogs.com/simple-focus/p/6879971.html
  */
class HBaseOnSparkWithBulkLoad {

  def commonInsert(): Unit ={
    val spark = SparkSession.builder().appName("commonInsert").master("local[4]").getOrCreate()

    val rdd = spark.sparkContext.textFile("/data/produce/2018-03-01.log")
    val data = rdd.map(_.split(",")).map{x => (x(0) + x(1), x(2))}
    val result = data.foreachPartition{x => {
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set(TableInputFormat.INPUT_TABLE, "data")
      hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "localhost")
      hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
      hbaseConf.addResource("/home/hadoop/data/lib/hbase-site.xml")
      val table = new HTable(hbaseConf, "data")
    }}
  }

}
