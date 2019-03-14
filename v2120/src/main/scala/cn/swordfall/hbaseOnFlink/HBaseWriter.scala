package cn.swordfall.hbaseOnFlink

import java.util
import java.util.ArrayList

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/1 1:34
  *
  * 写入HBase
  * 第一种：继承RichSinkFunction重写父类方法
  */
class HBaseWriter extends RichSinkFunction[String]{

  private var conn: Connection = null
  private var table: Table = null
  private val scan: Scan = null
  private val tableName: TableName = TableName.valueOf("test")
  private val cf1 = "cf1"

  /**
    * 建立HBase连接
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    val config:org.apache.hadoop.conf.Configuration = HBaseConfiguration.create
    config.set(HConstants.ZOOKEEPER_QUORUM, "192.168.187.201")
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
    config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)
    conn = ConnectionFactory.createConnection(config)
    table = conn.getTable(tableName)
  }

  /**
    * 处理获取的hbase数据
    * @param value
    * @param context
    */
  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val array: Array[String] = value.split(",")
    val put: Put = new Put(Bytes.toBytes(array(0)))
    put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("name"), Bytes.toBytes(array(1)))
    put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("age"), Bytes.toBytes(array(2)))
    val putList: util.ArrayList[Put] = new util.ArrayList[Put]
    //设置缓存1m，当达到1m时数据会自动刷到hbase
    val params: BufferedMutatorParams = new BufferedMutatorParams(tableName)
    //设置缓存的大小
    params.writeBufferSize(1024 * 1024)
    val mutator: BufferedMutator = conn.getBufferedMutator(params)
    mutator.mutate(putList)
    mutator.flush()
    putList.clear()
  }

  /**
    * 关闭
    */
  override def close(): Unit = {
    if (table != null) table.close()
    if (conn != null) conn.close()
  }
}
