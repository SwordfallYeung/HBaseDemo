package cn.swordfall.hbaseOnFlink

import java.util.{Date, Properties}

import org.apache.commons.net.ntp.TimeStamp
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
  * @Author: Yang JianQiu
  * @Date: 2019/2/22 11:24
  */
class HBaseOnBasicFlink {
  val zkServer = "192.168.187.201"
  val port = "2181"
  val tableName = TableName.valueOf("test")
  val cf1 = "cf1"
  val topic = "flink_topic"

  def readFromKafka: Unit ={
    val props = new Properties()
    props.put("bootstrap.servers", "192.168.187.201:9092")
    props.put("group.id", "kv_flink")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)

    val myConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema, props)
    val stream: DataStream[String] = env.addSource(myConsumer)
    stream.rebalance.map(x => {
      write2BasicHBase(x)
      (x)
    })
  }

  /**
    * 比较笨的方式，不是通用的，每来一条数据，建立一次连接并插入一条数据
    * @param value
    */
  def write2BasicHBase(value: String): Unit ={
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
}

object HBaseOnBasicFlink{
  def main(args: Array[String]): Unit = {
    val hbof = new HBaseOnBasicFlink
    hbof.readFromKafka
  }
}
