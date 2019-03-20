package cn.swordfall.hbaseOnFlink.flinkBatchProcessing

import cn.swordfall.hbaseOnFlink.{HBaseInputFormat, HBaseOutputFormat}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}

/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/14 23:49
  *
  *  flink dataSet 批处理读写HBase
  *  读取HBase数据方式：实现TableInputFormat接口，TableInputFormat实际上最终也是实现InputFormat接口的
  *  写入HBase方式：实现OutputFormat接口
  */
class HBaseOnFlinkBatchProcessing {

  /**
    * 读取HBase数据方式：实现TableInputFormat接口
    */
  def readFromHBaseWithTableInputFormat(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataStream = env.createInput(new HBaseInputFormat)
    dataStream.filter(_.f1.startsWith("20")).print()
  }

  /**
    * 写入HBase方式：实现OutputFormat接口
    */
  def write2HBaseWithOutputFormat(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //2.定义数据
    val dataSet: DataSet[String] = env.fromElements("103,zhangsan,20", "104,lisi,21", "105,wangwu,22", "106,zhaolilu,23")
    dataSet.output(new HBaseOutputFormat)
    //运行下面这句话，程序才会真正执行，这句代码针对的是data sinks写入数据的
    env.execute()
  }
}

object HBaseOnFlinkBatchProcessing{
  def main(args: Array[String]): Unit = {
    val hofbp = new HBaseOnFlinkBatchProcessing
    //hofbp.readFromHBaseWithTableInputFormat()
    hofbp.write2HBaseWithOutputFormat()
  }
}