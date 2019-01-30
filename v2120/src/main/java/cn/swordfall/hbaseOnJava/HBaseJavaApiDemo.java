package cn.swordfall.hbaseOnJava;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: Yang JianQiu
 * @Date: 2019/1/22 17:46
 */
public class HBaseJavaApiExample {

    /**
     * 声明静态配置
     */
    static Configuration conf = null;
    static Connection conn = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03");
        conf.set("hbase.zookeeper.property.client", "2181");
        try{
            conn = ConnectionFactory.createConnection(conf);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 创建只有一个列簇的表
     * @throws Exception
     */
    public static void createTable() throws Exception{
        Admin admin = conn.getAdmin();
        if (!admin.tableExists(TableName.valueOf("test"))){
            TableName tableName = TableName.valueOf("test");
            //表描述器构造器
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
            //列族描述器构造器
            ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("user"));
            //获得列描述器
            ColumnFamilyDescriptor cfd = cdb.build();
            //添加列族
            tdb.setColumnFamily(cfd);
            //获得表描述器
            TableDescriptor td = tdb.build();
            //创建表
            admin.createTable(td);
        }else {
            System.out.println("表已存在");
        }
        //关闭连接
    }

    /**
     * 创建表（包含多个列族）
     * @param tableName
     * @param columnFamilys
     * @throws Exception
     */
    public static void createTable(TableName tableName, String[] columnFamilys) throws Exception{
        Admin admin = conn.getAdmin();
        if (!admin.tableExists(tableName)){
            //表描述构造器
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
            //列族描述构造器
            ColumnFamilyDescriptorBuilder cdb;
            //获得列描述器
            ColumnFamilyDescriptor cfd;
            for (String columnFamily: columnFamilys){
                cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
                cfd = cdb.build();
                //添加列族
                tdb.setColumnFamily(cfd);
            }
            //获得表描述器
            TableDescriptor td = tdb.build();
            //创建表
            admin.createTable(td);
        }else {
            System.out.println("表已存在！");
        }
        //关闭链接
    }

    /**
     * 添加数据（多个rowKey，多个列族，适合由固定结构的数据）
     * @param tableName
     * @param list
     * @throws Exception
     */
    public static void insertMany(TableName tableName, List<Map<String, Object>> list) throws Exception{
        List<Put> puts = new ArrayList<Put>();
        //Table负责跟记录相关的操作如增删改查等
        Table table = conn.getTable(tableName);
        if (list != null && list.size() > 0){
            for (Map<String, Object> map: list){
                Put put = new Put(Bytes.toBytes(map.get("rowKey").toString()));
                put.addColumn(Bytes.toBytes(map.get("columnFamily").toString()), Bytes.toBytes(map.get("columnName").toString()), Bytes.toBytes(map.get("columnValue").toString()));
                puts.add(put);
            }
        }
        table.put(puts);
        table.close();
        System.out.println("add data Success!");
    }

    /**
     * 添加数据（多个rowKey，多个列族）
     * @throws Exception
     */
    public static void insertMany() throws Exception{
        Table table = conn.getTable(TableName.valueOf("test"));
        List<Put> puts = new ArrayList<Put>();
        Put put1 = new Put(Bytes.toBytes("rowKey1"));
        put1.addColumn(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes("wd"));

        Put put2 = new Put(Bytes.toBytes("rowKey2"));
        put2.addColumn(Bytes.toBytes("user"), Bytes.toBytes("age"), Bytes.toBytes("25"));

        Put put3 = new Put(Bytes.toBytes("rowKey3"));
        put3.addColumn(Bytes.toBytes("user"), Bytes.toBytes("weight"), Bytes.toBytes("60kg"));

        Put put4 = new Put(Bytes.toBytes("rowKey4"));
        put4.addColumn(Bytes.toBytes("user"), Bytes.toBytes("sex"), Bytes.toBytes("男"));

        puts.add(put1);
        puts.add(put2);
        puts.add(put3);
        puts.add(put4);
        table.put(puts);
        table.close();
    }

    /**
     * 根据RowKey , 列簇， 列名修改值
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @param columnValue
     * @throws Exception
     */
    public static void updateData(TableName tableName, String rowKey, String columnFamily, String columnName, String columnValue) throws Exception{
        Table table = conn.getTable(tableName);
        Put put1 = new Put(Bytes.toBytes(rowKey));
        put1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
        table.put(put1);
        table.close();
    }

    /**
     * 根据rowKey删除一行数据
     * @param tableName
     * @param rowKey
     * @throws Exception
     */
    public static void deleteData(TableName tableName, String rowKey) throws Exception{
        Table table = conn.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();
    }

    /**
     * 删除某一行的某一个列簇内容
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @throws Exception
     */
    public static void deleteData(TableName tableName, String rowKey, String columnFamily) throws Exception{
        Table table = conn.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addFamily(Bytes.toBytes(columnFamily));
        table.delete(delete);
        table.close();
    }

    /**
     * 删除某一行某个列簇某列的值
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @throws Exception
     */
    public static void deleteData(TableName tableName, String rowKey, String columnFamily, String columnName) throws Exception{
        Table table = conn.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        table.delete(delete);
        table.close();
    }

    /**
     * 删除某一行某个列簇多个列的值
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnNames
     * @throws Exception
     */
    public static void deleteData(TableName tableName, String rowKey, String columnFamily, List<String> columnNames) throws Exception{
        Table table = conn.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        for (String columnName: columnNames){
            delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        }
        table.delete(delete);
        table.close();
    }

    /**
     * 根据rowKey查询数据
     * @param tableName
     * @param rowKey
     * @throws Exception
     */
    public static void getResult(TableName tableName, String rowKey) throws Exception{
        Table table = conn.getTable(tableName);
        //获得一行
        Get get = new Get(Bytes.toBytes(rowKey));
        Result set = table.get(get);
        Cell[] cells = set.rawCells();
        for (Cell cell: cells){
            System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }
        table.close();
    }

    /**
     * 全表扫描
     * @param tableName
     * @throws Exception
     */
    public static void scanTable(TableName tableName) throws Exception{
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner rscan = table.getScanner(scan);
        for (Result rs : rscan){
            String rowKey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowKey);
            Cell[] cells = rs.rawCells();
            for (Cell cell: cells){
                System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "::"
                        + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::"
                        + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-------------------------------------------");
        }
    }

    //过滤器 LESS <  LESS_OR_EQUAL <=   EQUAL =   NOT_EQUAL <>   GREATER_OR_EQUAL >=   GREATER >   NO_OP 排除所有

    /**
     * rowKey过滤器
     * @param tableName
     * @throws Exception
     */
    public static void rowKeyFilter(TableName tableName) throws Exception{
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        //str$ 末尾匹配，相当于sql中的 %str  ^str开头匹配，相当于sql中的str%
        RowFilter filter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("Key1$"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs: scanner){
            String rowKey = Bytes.toString(rs.getRow());
            System.out.println("row key: " + rowKey);
            Cell[] cells = rs.rawCells();
            for (Cell cell: cells){
                System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "::"
                        + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::"
                        + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("--------------------------------------------");
        }
    }

    /**
     * 列值过滤器
     * @param tableName
     * @throws Exception
     */
    public static void singColumnFilter(TableName tableName) throws Exception{
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        //下列参数分别为列族，列名，比较符号，值
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("author"), Bytes.toBytes("name"),
                CompareOperator.EQUAL, Bytes.toBytes("spark"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs: scanner){
            String rowKey = Bytes.toString(rs.getRow());
            System.out.println("row key: " + rowKey);
            Cell[] cells = rs.rawCells();
            for (Cell cell: cells){
                System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "::"
                        + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::"
                        + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------------");
        }
    }

    /**
     * 列名前缀过滤器
     * @param tableName
     * @throws Exception
     */
    public static void columnPrefixFilter(TableName tableName) throws Exception{
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("name"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs: scanner){
            String rowKey = Bytes.toString(rs.getRow());
            System.out.println("row key:" + rowKey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells){
                System.out.println(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+"::"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    /**
     * 过滤器集合
     * @param tableName
     * @throws Exception
     */
    public static void filterSet(TableName tableName) throws Exception{
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("author"), Bytes.toBytes("name"),
                CompareOperator.EQUAL, Bytes.toBytes("spark"));
        ColumnPrefixFilter filter2 = new ColumnPrefixFilter(Bytes.toBytes("name"));
        list.addFilter(filter1);
        list.addFilter(filter2);

        scan.setFilter(list);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs: scanner){
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :"+rowkey);
            Cell[] cells  = rs.rawCells();
            for(Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+"::"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }

    }

    public static void main(String[] args) throws Exception{
        //创建表（只有一个列簇）
//        createTableOne();

        // 创建表(包含多个列簇)
        TableName tableName = TableName.valueOf("test");
        String[] columnFamilys = { "article", "author" };
//        createTable(tableName, columnFamilys);

        //添加数据
        List<Map<String,Object>> listMap=new ArrayList<Map<String, Object>>();
        Map<String,Object> map1=new HashMap<String,Object>();
        map1.put("rowKey","ce_shi1");
        map1.put("columnFamily","article");
        map1.put("columnName","title");
        map1.put("columnValue","Head First HBase");
        listMap.add(map1);
        Map<String,Object> map2=new HashMap<String,Object>();
        map2.put("rowKey","ce_shi1");
        map2.put("columnFamily","article");
        map2.put("columnName","content");
        map2.put("columnValue","HBase is the Hadoop database");
        listMap.add(map2);
        Map<String,Object> map3=new HashMap<String,Object>();
        map3.put("rowKey","ce_shi1");
        map3.put("columnFamily","article");
        map3.put("columnName","tag");
        map3.put("columnValue","Hadoop,HBase,NoSQL");
        listMap.add(map3);
        Map<String,Object> map4=new HashMap<String,Object>();
        map4.put("rowKey","ce_shi1");
        map4.put("columnFamily","author");
        map4.put("columnName","name");
        map4.put("columnValue","nicholas");
        listMap.add(map4);
        Map<String,Object> map5=new HashMap<String,Object>();
        map5.put("rowKey","ce_shi1");
        map5.put("columnFamily","author");
        map5.put("columnName","nickname");
        map5.put("columnValue","lee");
        listMap.add(map5);
        Map<String,Object> map6=new HashMap<String,Object>();
        map6.put("rowKey","ce_shi2");
        map6.put("columnFamily","author");
        map6.put("columnName","name");
        map6.put("columnValue","spark");
        listMap.add(map6);
        Map<String,Object> map7=new HashMap<String,Object>();
        map7.put("rowKey","ce_shi2");
        map7.put("columnFamily","author");
        map7.put("columnName","nickname");
        map7.put("columnValue","hadoop");
        listMap.add(map7);
//        insertMany(tableName,listMap);

        //根据RowKey，列簇，列名修改值
        String rowKey="ce_shi2";
        String columnFamily="author";
        String columnName="name";
        String columnValue="hbase";
//        updateData(tableName,rowKey,columnFamily,columnName,columnValue);


        String rowKey1="ce_shi1";
        String columnFamily1="article";
        String columnName1="name";
        List<String> columnNames=new ArrayList<String>();
        columnNames.add("content");
        columnNames.add("title");
        //删除某行某个列簇的某个列
//        deleteData(tableName,rowKey1,columnFamily1,columnName1);
        //删除某行某个列簇
//        deleteData(tableName,rowKey1,columnFamily1);
        //删除某行某个列簇的多个列
//        deleteData(tableName,rowKey1,columnFamily1,columnNames);
        //删除某行
//        deleteData(tableName,rowKey1);
        //根据rowKey查询数据
//        getResult(tableName,"rowKey1");

        //全表扫描
        scanTable(tableName);

        //rowKey过滤器
//        rowkeyFilter(tableName);

        //列值过滤器
//        singColumnFilter(tableName);

        //列名前缀过滤器
//        columnPrefixFilter(tableName);

        //过滤器集合
//        filterSet(tableName);
    }
}
