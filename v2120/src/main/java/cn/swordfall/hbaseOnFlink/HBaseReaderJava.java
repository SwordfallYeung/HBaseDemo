package cn.swordfall.hbaseOnFlink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author: Yang JianQiu
 * @Date: 2019/2/22 16:20
 *
 * 以HBase为数据源
 * 从HBase中获取数据，然后以流的形式发射
 *
 * 从HBase读取数据
 * 第一种：继承RichSourceFunction重写父类方法
 */
public class HBaseReaderJava extends RichSourceFunction<Tuple2<String, String>> {

    private static final Logger logger = LoggerFactory.getLogger(HBaseReaderJava.class);
    private Connection conn = null;
    private Table table = null;
    private Scan scan = null;
    private static TableName tableName = TableName.valueOf("test");
    private static final String cf1 = "cf1";

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        config.set(HConstants.ZOOKEEPER_QUORUM, "192.168.187.201");
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);

        conn = ConnectionFactory.createConnection(config);
        table = conn.getTable(tableName);
        scan = new Scan();
        scan.withStartRow(Bytes.toBytes("1001"));
        scan.withStopRow(Bytes.toBytes("1004"));
        scan.addFamily(Bytes.toBytes(cf1));
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> iterator = rs.iterator();
        while (iterator.hasNext()){
            Result result = iterator.next();
            String rowKey = Bytes.toString(result.getRow());
            StringBuffer sb = new StringBuffer();
            for (Cell cell: result.listCells()){
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                sb.append(value).append(",");
            }
            String valueString = sb.replace(sb.length() - 1, sb.length(), "").toString();
            Tuple2<String, String> tuple2 = new Tuple2<>();
            tuple2.setFields(rowKey, valueString);
            sourceContext.collect(tuple2);
        }
    }

    @Override
    public void cancel() {
        try {
            if (table != null){
                table.close();
            }
            if (conn != null){
                conn.close();
            }
        } catch (IOException e) {
            logger.error("Close HBase Exception:", e.toString());
        }
    }
}
