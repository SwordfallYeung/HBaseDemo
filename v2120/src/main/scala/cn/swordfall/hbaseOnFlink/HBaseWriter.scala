package cn.swordfall.hbaseOnFlink

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
  *
  * 注意：由于flink是一条一条的处理数据，所以我们在插入hbase的时候不能来一条flush下，
  * 不然会给hbase造成很大的压力，而且会产生很多线程导致集群崩溃，所以线上任务必须控制flush的频率。
  *
  * 解决方案：我们可以在open方法中定义一个变量，然后在写入hbase时比如500条flush一次，或者加入一个list，判断list的大小满足某个阀值flush一下
  */
class HBaseWriter extends RichSinkFunction[String]{

  var conn: Connection = null
  val scan: Scan = null
  var mutator: BufferedMutator = null
  var count = 0

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

    val tableName: TableName = TableName.valueOf("test")
    val params: BufferedMutatorParams = new BufferedMutatorParams(tableName)
    //设置缓存1m，当达到1m时数据会自动刷到hbase
    params.writeBufferSize(1024 * 1024) //设置缓存的大小
    mutator = conn.getBufferedMutator(params)
    count = 0
  }

  /**
    * 处理获取的hbase数据
    * @param value
    * @param context
    */
  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val cf1 = "cf1"
    val array: Array[String] = value.split(",")
    val put: Put = new Put(Bytes.toBytes(array(0)))
    put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("name"), Bytes.toBytes(array(1)))
    put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("age"), Bytes.toBytes(array(2)))
    mutator.mutate(put)
    //每满2000条刷新一下数据
    if (count >= 2000){
      mutator.flush()
      count = 0
    }
    count = count + 1
  }

  /**
    * 关闭
    */
  override def close(): Unit = {
    if (conn != null) conn.close()
  }
}
