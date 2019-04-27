package cn.swordfall.hbaseOnFlink;/**
 * @Author: Yang JianQiu
 * @Date: 2019/2/23 23:37
 */

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;

/**
 * @author: Swordfall Yeung
 * @date:
 * @desc:
 *
 * 写入HBase
 * 第一种：继承RichSinkFunction重写父类方法
 */
public class HBaseWriterJava extends RichSinkFunction<String> {
    private static final Logger logger = LoggerFactory.getLogger(HBaseReaderJava.class);
    private Connection conn = null;
    private static TableName tableName = TableName.valueOf("test");
    private static final String cf1 = "cf1";
    private BufferedMutator mutator;
    private int count;

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        config.set(HConstants.ZOOKEEPER_QUORUM, "192.168.187.201");
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);
        conn = ConnectionFactory.createConnection(config);

        BufferedMutatorParams params = new BufferedMutatorParams(tableName);
        //设置缓存1m，当达到1m时数据会自动刷到hbase
        params.writeBufferSize(1024 * 1024); //设置缓存的大小
        mutator = conn.getBufferedMutator(params);
        count = 0;
    }

    @Override
    public void invoke(String record, Context context) throws Exception {
        String[] array = record.split(",");
        Put put = new Put(Bytes.toBytes(array[0]));
        put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("name"), Bytes.toBytes(array[1]));
        put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("age"), Bytes.toBytes(array[2]));
        mutator.mutate(put);
        //每满2000条刷新一下数据
        if (count >= 2000){
            mutator.flush();
            count = 0;
        }
        count++;
    }

    @Override
    public void close() throws Exception {
        if (conn != null){
            conn.close();
        }
    }
}
