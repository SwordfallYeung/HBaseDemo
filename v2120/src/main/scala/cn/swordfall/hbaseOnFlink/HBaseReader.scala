package cn.swordfall.hbaseOnFlink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes

/**
  * @Author: Yang JianQiu
  * @Date: 2019/2/28 18:05
  *
  * 以HBase为数据源
  * 从HBase中获取数据，然后以流的形式发射
  *
  * 从HBase读取数据
  * 第一种：继承RichSourceFunction重写父类方法
  */
class HBaseReader extends RichSourceFunction[(String, String)]{

  private var conn: Connection = null
  private var table: Table = null
  private var scan: Scan = null
  private var tableName: TableName = TableName.valueOf("test")
  private var cf1: String = null

  /**
    * 在open方法使用HBase的客户端连接
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    val config: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create()

    config.set(HConstants.ZOOKEEPER_QUORUM, "192.168.187.201")
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
    config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)

    conn = ConnectionFactory.createConnection(config)
    table = conn.getTable(tableName)
    scan = new Scan()
    scan.withStartRow(Bytes.toBytes("1001"))
    scan.withStopRow(Bytes.toBytes("1004"))
    scan.addFamily(Bytes.toBytes(cf1))
  }

  /**
    * run方法来自java的接口文件SourceFunction，使用IDEA工具Ctrl + o 无法便捷获取到该方法，直接override会提示
    * @param sourceContext
    */
  override def run(sourceContext: SourceContext[(String, String)]): Unit = {
    val rs = table.getScanner(scan)
    val iterator = rs.iterator()
    while (iterator.hasNext){
      val result = iterator.next()
      val rowKey = Bytes.toString(result.getRow)
      val sb: StringBuffer = new StringBuffer();
      for (cell:Cell <- result.listCells()){
        val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
        sb.append(value).append(",")
      }
      val valueString = sb.replace(sb.length() - 1, sb.length(), "").toString
      sourceContext.collect((rowKey, valueString))
    }
  }

  /**
    * 关闭hbase的连接，关闭table表
    */
  override def close(): Unit = {
    try {
      if (table != null) {
        table.close()
      }
      if (conn != null) {
        conn.close()
      }
    } catch {
      case e:Exception => println(e.getMessage)
    }
  }
}
