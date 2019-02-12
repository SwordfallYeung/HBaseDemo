package cn.swordfall.hbaseOnSpark

import java.util.Base64

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job

/**
  * @Author: Yang JianQiu
  * @Date: 2019/1/25 2:27
  * HBaseContext Usage Example
  * 参考资料：https://my.oschina.net/uchihamadara/blog/2032481
  */
class HBaseOnBasicSpark {

  /** spark 往hbase里面写入数据 start **/

  /**
    * saveAsHadoopDataset
    */
  def writeToHBase(): Unit ={
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    /* spark2.0以前的写法
     val conf = new SparkConf().setAppName("SparkToHBase").setMaster("local")
     val sc = new SparkContext(conf)
    */
    val sparkSession = SparkSession.builder().appName("SparkToHBase").master("local[4]").getOrCreate()
    val sc = sparkSession.sparkContext

    val tableName = "test"

    //创建HBase配置
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.187.201") //设置zookeeper集群，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181") //设置zookeeper连接端口，默认2181
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    //初始化job，设置输出格式，TableOutputFormat 是 org.apache.hadoop.hbase.mapred 包下的
    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    val dataRDD = sc.makeRDD(Array("12,jack,16", "11,Lucy,15", "15,mike,17", "13,Lily,14"))

    val data = dataRDD.map{ item =>
      val Array(key, name, age) = item.split(",")
      val rowKey = key.reverse
      val put = new Put(Bytes.toBytes(rowKey))
      /*一个Put对象就是一行记录，在构造方法中指定主键
      * 所有插入的数据 须用 org.apache.hadoop.hbase.util.Bytes.toBytes 转换
      * Put.addColumn 方法接收三个参数：列族，列名，数据*/
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes(name))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(age))
      (new ImmutableBytesWritable(), put)
    }
    //保存到HBase表
    data.saveAsHadoopDataset(jobConf)
    sparkSession.stop()
  }

  /**
    * saveAsNewAPIHadoopDataset
    */
  def writeToHBaseNewAPI(): Unit ={
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val sparkSession = SparkSession.builder().appName("SparkToHBase").master("local[4]").getOrCreate()
    val sc = sparkSession.sparkContext

    val tableName = "test"

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.187.201")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.OUTPUT_TABLE, tableName)

    val jobConf = new JobConf(hbaseConf)
    //设置job的输出格式
    val job = Job.getInstance(jobConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[ImmutableBytesWritable]])

    val input = sc.textFile("v2120/a.txt")

    val data = input.map{item =>
      val Array(key, name, age) = item.split(",")
      val rowKey = key.reverse
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes(name))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(age))
      (new ImmutableBytesWritable, put)
    }
    //保存到HBase表
    data.saveAsNewAPIHadoopDataset(job.getConfiguration)
    sparkSession.stop()
  }

  /** spark 往hbase里面存放数据 end **/

  /** spark 从hbase里面读取数据 start **/

  /**
    * take
    */
  def readFromHBaseWithHBaseNewAPI(): Unit ={
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val sparkSession = SparkSession.builder().appName("SparkToHBase").master("local[4]").getOrCreate()
    val sc = sparkSession.sparkContext

    val tableName = "test"

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.187.201")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181") //设置zookeeper连接端口，默认2181
    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, tableName)

    //读取数据并转化成rdd TableInputFormat是org.apache.hadoop.hbase.mapreduce包下的
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    hbaseRDD.foreach{ case (_, result) =>
      //获取行健
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val name = Bytes.toString(result.getValue("cf1".getBytes(), "name".getBytes()))
      val age = Bytes.toString(result.getValue("cf1".getBytes(), "age".getBytes()))
        println("Row key:" + key + "\tcf1.Name:" + name + "\tcf1.Age:" + age)
    }

    sparkSession.stop()
  }

  /**
    * scan
    */
  def readFromHBaseWithHBaseNewAPIScan(): Unit ={
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val sparkSession = SparkSession.builder().appName("SparkToHBase").master("local").getOrCreate()
    val sc = sparkSession.sparkContext

    val tableName = "test"

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.187.201")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, tableName)

    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("cf1"))
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = new String(Base64.getEncoder.encode(proto.toByteArray))
    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.SCAN, scanToString)

    //读取数据并转化成rdd TableInputFormat是org.apache.hadoop.hbase.mapreduce包下的
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val dataRDD = hbaseRDD
      .map(x => x._2)
      .map{result =>
        (result.getRow, result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name")), result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age")))
      }.map(row => (new String(row._1), new String(row._2), new String(row._3)))
      .collect()
      .foreach(r => (println("rowKey:"+r._1 + ", name:" + r._2 + ", age:" + r._3)))
  }
}
