package cn.swordfall.hbaseOnFlink

import java.io.IOException

import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
  * @Author: Yang JianQiu
  * @Date: 2019/3/1 1:14
  *
  * 从HBase读取数据
  * 第二种：实现TableInputFormat接口
  */
class HBaseInputFormat extends TableInputFormat[(String, String)]{

  private val tableName: TableName = TableName.valueOf("test")
  private val cf1: String = "cf1"
  private var conn: Connection = null

  override def configure(parameters: Configuration): Unit = {

    val config: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create

    config.set(HConstants.ZOOKEEPER_QUORUM, "192.168.187.201")
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
    config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)

    try {
      conn = ConnectionFactory.createConnection(config)
      table = conn.getTable(tableName).asInstanceOf[HTable]
      scan = new Nothing
      scan.withStartRow(Bytes.toBytes("1001"))
      scan.withStopRow(Bytes.toBytes("1004"))
      scan.addFamily(Bytes.toBytes(cf1))
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

  override def mapResultToTuple(result: Result): (String, String) = {
    val rowKey = Bytes.toString(result.getRow)
    val sb = new StringBuffer()
    for (cell: Cell <- result.listCells()){
      val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
      sb.append(value).append(",")
    }
    val value = sb.replace(sb.length() - 1, sb.length(), "").toString
    (rowKey, value)
  }

  override def getTableName: String = {
    "test"
  }

  override def getScanner: Scan = {
    scan
  }

  override def close(): Unit = {
    try {
      if (table != null) table.close
      if (conn != null) conn.close
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }
}
