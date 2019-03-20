package cn.swordfall.hbaseOnFlink.flinkSteaming

import java.util.{Date, Properties}

import cn.swordfall.hbaseOnFlink._
import org.apache.commons.net.ntp.TimeStamp
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

/**
  * @Author: Yang JianQiu
  * @Date: 2019/2/22 11:24
  *
  * 从HBase读取数据有两种方式
  * 第一种：继承RichSourceFunction重写父类方法
  * 第二种：实现TableInputFormat接口
  *
  * 写入HBase提供两种方式：
  * 第一种：继承RichSinkFunction重写父类方法
  * 第二种：实现OutputFormat接口
  *
  * 参考资料：
  * https://blog.csdn.net/liguohuabigdata/article/details/78588861
  */
class HBaseOnFlinkStreaming {

  /** ****************************** read start ***************************************/

  /**
    * 从HBase读取数据
    * 第一种：继承RichSourceFunction重写父类方法
    */
  def readFromHBaseWithRichSourceFunction(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val dataStream: DataStream[(String, String)] = env.addSource(new HBaseReader)
    dataStream.map(x => println(x._1 + " " + x._2))
    env.execute()
  }

  /**
    * 从HBase读取数据
    * 第二种：实现TableInputFormat接口
    */
  def readFromHBaseWithTableInputFormat(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val dataStream = env.createInput(new HBaseInputFormat)
    dataStream.filter(_.f0.startsWith("10")).print()
    env.execute()
  }

  /** ****************************** read end ***************************************/

  /** ****************************** write start ***************************************/
  def readFromKafka: Unit ={
    val topic = "test"
    val props = new Properties()
    props.put("bootstrap.servers", "192.168.187.201:9092")
    props.put("group.id", "kv_flink")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)

    val myConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema, props)
    val stream = env.addSource(myConsumer)
    stream.rebalance.map(x => {
      write2BasicHBase(x)
      x
    })
  }

  /**
    * 比较笨的方式，不是通用的，每来一条数据，建立一次连接并插入一条数据
    * @param value
    */
  def write2BasicHBase(value: String): Unit ={
    val tableName = TableName.valueOf("test")
    val cf1 = "cf1"
    val zkServer = "192.168.187.201"
    val port = "2181"

    val config = HBaseConfiguration.create

    config.set(HConstants.ZOOKEEPER_QUORUM, zkServer)
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, port)
    config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
    config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)

    val conn = ConnectionFactory.createConnection(config)
    val admin = conn.getAdmin
    if (!admin.tableExists(tableName)) {
      val tdb = TableDescriptorBuilder.newBuilder(tableName)
      val cfdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf1))
      val cfd = cfdb.build
      tdb.setColumnFamily(cfd)
      val td = tdb.build
      admin.createTable(td)
    }
    val table = conn.getTable(tableName)
    val ts = new TimeStamp(new Date)
    val date = ts.getDate
    val put = new Put(Bytes.toBytes(date.getTime))
    put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("name"), Bytes.toBytes(value))
    table.put(put)
    table.close()
    conn.close()
  }

  /**
    * 写入HBase
    * 第一种：继承RichSinkFunction重写父类方法
    */
  def write2HBaseWithRichSinkFunction(): Unit = {
    val topic = "test"
    val props = new Properties
    props.put("bootstrap.servers", "192.168.187.201:9092")
    props.put("group.id", "kv_flink")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val myConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema, props)
    val dataStream: DataStream[String] = env.addSource(myConsumer)
    //写入HBase
    dataStream.addSink(new HBaseWriter)
    env.execute()
  }

  /**
    * 写入HBase
    * 第二种：实现OutputFormat接口
    */
  def write2HBaseWithOutputFormat(): Unit = {
    val topic = "test"
    val props = new Properties
    props.put("bootstrap.servers", "192.168.187.201:9092")
    props.put("group.id", "kv_flink")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val myConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema, props)
    val dataStream: DataStream[String] = env.addSource(myConsumer)
    dataStream.writeUsingOutputFormat(new HBaseOutputFormat)
    env.execute()
  }
}

object HBaseOnFlinkStreaming{
  def main(args: Array[String]): Unit = {
    val hbof = new HBaseOnFlinkStreaming
    //hbof.readFromHBaseWithRichSourceFunction
    //hbof.readFromHBaseWithTableInputFormat()
    //hbof.write2HBaseWithRichSinkFunction()
    hbof.write2HBaseWithOutputFormat()
  }
}
