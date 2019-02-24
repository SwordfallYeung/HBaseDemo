package cn.swordfall.hbaseOnFlink;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Author: Yang JianQiu
 * @Date: 2019/2/23 22:08
 *
 * 从HBase读取数据
 * 第二种：实现TableInputFormat接口
 */
public class HBaseInputFormatJava extends TableInputFormat<Tuple2<String, String>> {

    private static TableName tableName = TableName.valueOf("test");
    private static final String cf1 = "cf1";
    private Connection conn = null;

    @Override
    protected Tuple2<String, String> mapResultToTuple(Result result) {
        String rowKey = Bytes.toString(result.getRow());
        StringBuffer sb = new StringBuffer();
        for (Cell cell : result.listCells()){
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            sb.append(value).append(",");
        }
        String value = sb.replace(sb.length() -1 , sb.length(), "").toString();
        Tuple2<String, String> tuple2 = new Tuple2<>();
        tuple2.setField(rowKey, 0);
        tuple2.setField(value, 1);
        return tuple2;
    }

    @Override
    protected String getTableName() {
        return "test";
    }

    @Override
    protected Scan getScanner() {
        return scan;
    }

    @Override
    public void configure(Configuration parameters) {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        config.set(HConstants.ZOOKEEPER_QUORUM, "192.168.187.201");
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);

        try {
            conn = ConnectionFactory.createConnection(config);
            table = (HTable) conn.getTable(tableName);
            scan = new Scan();
            scan.withStartRow(Bytes.toBytes("1001"));
            scan.withStopRow(Bytes.toBytes("1004"));
            scan.addFamily(Bytes.toBytes(cf1));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        if (table != null){
            table.close();
        }
        if (conn != null){
            conn.close();
        }
    }
}
