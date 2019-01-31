package cn.swordfall.hbaseOnSpark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.SparkSession

/**
  * @Author: Yang JianQiu
  * @Date: 2019/1/31 18:07
  */
class HBaseOnSparkWithBulkLoad {

  def commonInsert(): Unit ={
    val spark = SparkSession.builder().appName("commonInsert").master("local[4]").getOrCreate()

    val rdd = spark.sparkContext.textFile("/data/produce/2018-03-01.log")
    val data = rdd.map(_.split(",")).map{x => (x(0) + x(1), x(2))}
    val result = data.foreachPartition{x => {
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set(TableInputFormat)
    }}
  }

}
