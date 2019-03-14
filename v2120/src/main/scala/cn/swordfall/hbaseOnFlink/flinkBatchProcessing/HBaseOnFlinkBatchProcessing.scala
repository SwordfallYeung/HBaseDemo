package cn.swordfall.hbaseOnFlink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/14 23:49
  */
class HBaseBatch {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.createInput(new HBaseInputFormat)
    
    //2.定义数据 stu(age, name, height)
    val dataSet: DataSet[String] = env.fromElements("zhangsan", "lisi", "wangwu", "zhaolilu")
    dataSet.output(new HBaseOutputFormat)

  }
}
