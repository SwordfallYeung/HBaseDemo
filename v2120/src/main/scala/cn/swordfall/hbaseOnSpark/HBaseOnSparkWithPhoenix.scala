package cn.swordfall.hbaseOnSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @Author: Yang JianQiu
  * @Date: 2019/1/31 2:11
  * 参考资料：
  * https://www.cnblogs.com/wumingcong/p/6044038.html
  */
class HBaseOnSparkWithPhoenix {

  def readFromHBaseWithPhoenix: Unit ={
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkSession = SparkSession.builder().appName("SparkHBaseDataFrame").master("local").getOrCreate()

    val url = s"jdbc:phoenix:localhost:2181"
    val dbTable = "PHOENIXTEST"

    //spark 读取 phoenix 返回 DataFrame的第一种方式
    val rdf = sparkSession.read
      .format("jdbc")
      .option("driver", "org.apache.phoenix,jdbc,PhoenixDriver")
      .option("url", url)
      .option("dbtable", dbTable)
      .load()

    rdf.printSchema()

    //spark 读取 phoenix 返回 DataFrame的第二种方式
    val df = sparkSession.read
      .format("org.apache.phoenix.spark")
      .options(Map("table" -> dbTable, "zkUrl" -> url))
      .load()
    df.printSchema()

    //spark DataFrame 写入 phoenix，需要先建好表
    df.write
      .format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite)
      .options(Map("table" -> "PHOENIXTESTCOPY", "zkUrl" -> url))
      .save()

    sparkSession.stop()
  }
}
