package cn.swordfall.hbaseOnSpark

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: Yang JianQiu
  * @Date: 2019/1/25 2:27
  * HBaseContext Usage Example
  * This example shows how HBaseContext can be used to do a foreachPartition on a RDD in Scala:
  */
class HBaseOnBasicSpark {

  def main(args: Array[String]): Unit = {

  }

  /** spark 往hbase里面存放数据 **/

  /**
    * saveAsHadoopDataset
    */
  def writeToHBase(): Unit ={

    val conf = new SparkConf().setAppName("SparkToHBase").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile("a.txt")

    //创建HBase配置
    val hConf = HBaseConfiguration.create()
    hConf.set(HConstants.ZOOKEEPER_QUORUM, "local:2181")

    //创建JobConf，设置输出格式和表名
    val jobConf = new JobConf(hConf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "test")

    val data = input.map{ item =>
      val Array(key, value) = item.split("\t")
      val rowKey = key.reverse
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("info"), Bytes.toBytes(value))
      (new ImmutableBytesWritable(), put)
    }
    //保存到HBase表
    data.saveAsHadoopDataset(jobConf)
    sc.stop()
  }

  /**
    * saveAsNewAPIHadoopDataset
    */
  def writeToHBaseNewAPI(): Unit ={
    val conf = new SparkConf().setAppName("SparkToHBase").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile("a.txt")

    val hConf = HBaseConfiguration.create()
    hConf.set(HConstants.ZOOKEEPER_QUORUM, "localhost:2181")

    val jobConf = new JobConf(hConf, this.getClass)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "test")
    //设置job的输出格式
    val job = Job.getInstance(jobConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[ImmutableBytesWritable]])
    val data = input.map{item =>
      val Array(key, value) = item.split("\t")
      val rowKey = key.reverse
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("info"), Bytes.toBytes(value))
      (new ImmutableBytesWritable, put)
    }
    //保存到HBase表
    data.saveAsNewAPIHadoopDataset(job.getConfiguration)
    sc.stop()
  }
}
