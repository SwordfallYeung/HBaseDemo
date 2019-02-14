package cn.swordfall.hbaseOnSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

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
}
