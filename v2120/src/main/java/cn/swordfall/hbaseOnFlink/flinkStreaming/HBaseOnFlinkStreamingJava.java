package cn.swordfall.hbaseOnFlink.flinkStreaming;

import cn.swordfall.hbaseOnFlink.HBaseInputFormatJava;
import cn.swordfall.hbaseOnFlink.HBaseOutputFormatJava;
import cn.swordfall.hbaseOnFlink.HBaseReaderJava;
import cn.swordfall.hbaseOnFlink.HBaseWriterJava;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;
import java.util.Properties;

/**
 * @Author: Yang JianQiu
 * @Date: 2019/2/22 11:04
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
 * https://blog.csdn.net/javajxz008/article/details/83269108
 * https://blog.csdn.net/javajxz008/article/details/83182063
 * https://blog.csdn.net/Mathieu66/article/details/83095189
 */
public class HBaseOnFlinkStreamingJava {
    private static String zkServer = "192.168.187.201";
    private static String port = "2181";
    private static TableName tableName = TableName.valueOf("test");
    private static final String cf1 = "cf1";
    private static final String topic = "flink_topic";

    public static void main(String[] args) {
        readFromKafkaAndWrite();
    }

    /******************************** read start ***************************************/

    /**
     * 从HBase读取数据
     * 第一种：继承RichSourceFunction重写父类方法
     * @throws Exception
     */
    public static void readFromHBaseWithRichSourceFunction() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        DataStream<Tuple2<String, String>> dataStream = env.addSource(new HBaseReaderJava());

        dataStream.map(new MapFunction<Tuple2<String,String>, Object>() {
            @Override
            public Object map(Tuple2<String, String> value) throws Exception {
                System.out.println(value.f0 + " " + value.f1);
                return null;
            }
        });
    }

    /**
     * 从HBase读取数据
     * 第二种：实现TableInputFormat接口
     */
    public static void readFromHBaseWithTableInputFormat(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        DataStream<Tuple2<String, String>> dataStream = env.createInput(new HBaseInputFormatJava());

        dataStream.filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> tuple2) throws Exception {
                return tuple2.f0.startsWith("someStr");
            }
        });
    }

    /******************************** read end ***************************************/

    /******************************** write start ***************************************/

    /**
     * 从Kafka读取数据
     */
    public static void readFromKafkaAndWrite(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.187.201:9092");
        props.put("group.id", "kv_flink");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
        DataStream<String> stream = env.addSource(myConsumer);

        //写入hbase，比较笨的方法一
        stream.rebalance().map(new MapFunction<String, Object>() {
            @Override
            public Object map(String value) throws Exception {
                write2HBase(value);
                return value;
            }
        });

        //
    }

    /**
     * 比较笨的方式，不是通用的，每来一条数据，建立一次连接并插入一条数据
     * 会产生频繁的连接打开和关闭
     * @param value
     */
    public static void write2HBase(String value) throws Exception{
        Configuration config = HBaseConfiguration.create();

        config.set(HConstants.ZOOKEEPER_QUORUM, zkServer);
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, port);
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);

        Connection conn = ConnectionFactory.createConnection(config);
        Admin admin = conn.getAdmin();
        if (!admin.tableExists(tableName)){
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
            ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf1));
            ColumnFamilyDescriptor cfd = cfdb.build();
            tdb.setColumnFamily(cfd);
            TableDescriptor td = tdb.build();
            admin.createTable(td);
        }
        Table table = conn.getTable(tableName);
        TimeStamp ts = new TimeStamp(new Date());
        Date date = ts.getDate();
        Put put = new Put(Bytes.toBytes(date.getTime()));
        put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("name"), Bytes.toBytes(value));
        table.put(put);
        table.close();
        conn.close();
    }

    /**
     * 写入HBase
     * 第一种：继承RichSinkFunction重写父类方法
     */
    public void write2HBaseWithRichSinkFunction(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.187.201:9092");
        props.put("group.id", "kv_flink");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
        DataStream<String> dataStream = env.addSource(myConsumer);
        //写入HBase
        dataStream.addSink(new HBaseWriterJava());
    }

    /**
     * 写入HBase
     * 第二种：实现OutputFormat接口
     */
    public void write2HBaseWithOutputFormat(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.187.201:9092");
        props.put("group.id", "kv_flink");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
        DataStream<String> dataStream = env.addSource(myConsumer);
        //写入HBase
        dataStream.writeUsingOutputFormat(new HBaseOutputFormatJava());
    }

    /******************************** write end ***************************************/
}
