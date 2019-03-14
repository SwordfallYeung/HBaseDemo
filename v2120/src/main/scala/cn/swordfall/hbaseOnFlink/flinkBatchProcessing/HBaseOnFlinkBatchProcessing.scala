package cn.swordfall.hbaseOnFlink.flinkBatchProcessing

import cn.swordfall.hbaseOnFlink.{HBaseInputFormat, HBaseOutputFormat}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/14 23:49
  *
  *  flink dataSet 批处理读写HBase
  *  读取HBase数据方式：实现TableInputFormat接口
  *  写入HBase方式：实现OutputFormat接口
  */
class HBaseOnFlinkBatchProcessing {

  /**
    * 读取HBase数据方式：实现TableInputFormat接口
    */
  def readFromHBaseWithTableInputFormat(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataStream = env.createInput(new HBaseInputFormat)
    dataStream.filter(x => x._1.startsWith("someStr")).print()
  }

  /**
    * 写入HBase
    * 第二种：实现OutputFormat接口
    */
  def write2HBaseWithOutputFormat(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //2.定义数据
    val dataSet: DataSet[String] = env.fromElements("zhangsan", "lisi", "wangwu", "zhaolilu")
    dataSet.output(new HBaseOutputFormat)
  }
}
