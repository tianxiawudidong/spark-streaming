package com.ifchange.sparkstreaming.v1.hbase;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HbaseClient {

    private Connection connection;
    private Admin admin = null;
    private volatile boolean isBusy = false;
    private Table table = null;
    private TableName tableName;
    private List<Put> putlist = new ArrayList<>();
    private List<Delete> deleteList = new ArrayList<>();
    private Scan scan = new Scan();

    public HbaseClient(String hosts, String port) throws Exception {
        init(hosts, port);
    }

    public HbaseClient(String hosts) throws Exception {
        init(hosts, null);
    }

    public HbaseClient() throws Exception {
        init(null, null);
    }

    //初始化，创建连接
    private void init(String hosts, String port) {
        try {
            if (port == null && hosts == null) {
                connection = HbaseConnFactory.createConn();
            } else if (port == null) {
                connection = HbaseConnFactory.createConn(hosts);
            } else {
                connection = HbaseConnFactory.createConn(hosts, port);
            }
            admin = connection.getAdmin();
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            System.out.println("*********hbase connect " + hosts + ":" + port + " success......");
//           try {
//                //list table
//                for (String table_name : listTables()) {
//                    System.out.println("******************table_name:" + table_name);
//                }
//            } catch (Exception ex) {
//                ex.printStackTrace();
//            }
        }
    }

    //关闭连接
    public void close() throws Exception {
        if (null != admin) {
            admin.close();
        }
        if (null != table) {
            table.close();
        }
        if (null != connection) {
            connection.close();
        }
    }

    public void setTbale(String tablename) throws Exception {
        tableName = TableName.valueOf(tablename);
        if (table != null) {
            table.close();
        }
        table = connection.getTable(tableName);
    }

    private void checkTable() throws Exception {
        if (tableName == null) {
            throw new Exception("tableName is null,please use function 'setTable' to set tableName");
        }
    }

    public String getTableName() {
        return tableName.getNameAsString();
    }

    //建表
    public void createTable(String tablename, String[] cols, int Version) throws Exception {
        TableName tn = TableName.valueOf(tablename);
        if (admin.tableExists(tn)) {
            throw new Exception("talbe is exists!");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tn);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
                hColumnDescriptor.setVersions(0, Version);
            }
            admin.createTable(hTableDescriptor);
            setTbale(tablename);
        }
    }

    public void createTable(String tablename, String[] cols) throws Exception {
        createTable(tablename, cols, 1);
    }

    //删表
    public void deleteTable() throws Exception {
        checkTable();
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    //查看已有表
    public List<String> listTables() throws Exception {
        HTableDescriptor hTableDescriptors[] = admin.listTables();
        List<String> tableNames = new ArrayList<String>(hTableDescriptors.length);
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            System.out.println("names:" + hTableDescriptor.getNameAsString());
            tableNames.add(hTableDescriptor.getNameAsString());
        }
        return tableNames;
    }

    //插入数据
    public void inster(String rowkey, String colName, String val) throws Exception {
        checkTable();
        String[] clos = new String[2];
        clos[1] = null;
        clos = colName.split(":");
        String colFamily = clos[0];
        String col = clos[1];
        Put put = new Put(Bytes.toBytes(rowkey));
        if (col != null) {
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        } else {
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        }
        table.put(put);
    }

    //插入数据  
    public void insterRow(String tableName, String rowkey, String colFamily, String col, String val) throws IOException {
        try {
            checkTable();
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    //批量插入
    public void insters(String rowkey, String colName, String val) throws Exception {
        checkTable();
        String[] clos = colName.split(":");
        String colFamily = clos[0];
        String col = "";
        try {
            col = clos[1];
        } catch (ArrayIndexOutOfBoundsException e) {
            //col = null; 列标示符不存在
        }
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        putlist.add(put);
    }

    //批量插入
    public void insters2(String rowkey, String colFamily, String colName, String val) throws Exception {
        checkTable();
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colName), Bytes.toBytes(val));
        putlist.add(put);
    }

    public void insterByMap(String rowkey, String colFamily, Map<String,String> map) throws Exception {
        checkTable();
        Put put = new Put(Bytes.toBytes(rowkey));
        Set<String> keySet = map.keySet();
        for(String key:keySet){
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(key), Bytes.toBytes(map.get(key)));
        }
        putlist.add(put);
    }

    /*
     * 批量插入数据  
     * @param rowkey
     * @param colFamily
     * @param cols
     * @throws IOException
     */
    public void insterBatchRows(String rowkey, String colFamily, String[] cols, String[] vals) throws Exception {
        checkTable();
        Put put = new Put(Bytes.toBytes(rowkey));
        //批量插入
        for (int i = 0; i < cols.length; i++) {
            String col = cols[i];
            String value = vals[i];
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(value));
        }
        putlist.add(put);
    }

    /*
     * 批量插入数据  
     * @param tableName
     * @param rowkey
     * @param colFamily
     * @param cols
     * @throws IOException
     */
    public void insterBatchRows2(String rowkey, String colFamily, String[] cols, String[] vals) throws Exception {
        checkTable();
        //批量插入
        List<Put> putList = new ArrayList<>();
        for (int i = 0; i < cols.length; i++) {
            Put put = new Put(Bytes.toBytes(rowkey));
            String col = cols[i];
            String value = vals[i];
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(value));
            putList.add(put);
        }
        table.put(putList);
    }

    //执行批量插入动作
    public void saveInsert() throws Exception {
        table.put(putlist);
        putlist.clear();
    }

    public List<Put> getPutList() {
        return putlist;
    }

    //删除数据
    public void deleRow(String rowkey, String colFamily, String col) throws Exception {
        checkTable();
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        if (colFamily != null) {
            //删除指定列族
            delete.addFamily(Bytes.toBytes(colFamily));
        }
        if (col != null) {
            //删除指定列
            delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        }
        table.delete(delete);
    }

    public void deleteRow(String rowkey, String colFamily) throws Exception {
        deleRow(rowkey, colFamily, null);
    }

    public void deleteRow(String rowkey) throws Exception {
        deleRow(rowkey, null, null);
    }

    //批量删除
    public void deleRows(String rowkey, String colFamily, String col) throws Exception {
        checkTable();
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        if (colFamily != null) {
            //删除指定列族
            delete.addFamily(Bytes.toBytes(colFamily));
        }
        if (col != null) {
            //删除指定列
            delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        }
        deleteList.add(delete);
    }

    public void deleteRows(String rowkey, String colFamily) throws Exception {
        deleRows(rowkey, colFamily, null);
    }

    public void deleteRows(String rowkey) throws Exception {
        deleRows(rowkey, null, null);
    }

    public void saveDelete() throws Exception {
        table.delete(deleteList);
        deleteList.clear();
    }

    //根据rowkey查找数据
    public Result getData(String rowkey, String colFamily, String col) throws Exception {
        checkTable();
        Get get = new Get(Bytes.toBytes(rowkey));
        if (col != null && colFamily != null) {
            get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        } else if (colFamily != null) {
            get.addFamily(Bytes.toBytes(colFamily));
        }
        Result result = table.get(get);
        return result;
    }

    public Result getData(String rowkey) throws Exception {
        return getData(rowkey, null, null);
    }

    public Result getData(String rowkey, String colFamily) throws Exception {
        return getData(rowkey, colFamily, null);
    }

    //根据rowkey查找所有版本的数据
    public Result getAllData(String rowkey, String colFamily, String col) throws Exception {
        checkTable();
        Get get = new Get(Bytes.toBytes(rowkey));
        if (col != null && colFamily != null) {
            get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        } else if (colFamily != null) {
            get.addFamily(Bytes.toBytes(colFamily));
        }
        get.setMaxVersions(10);
        Result result = table.get(get);
        return result;
    }

    public Result getAllData(String rowkey, String colFamily) throws Exception {
        return getAllData(rowkey, colFamily, null);
    }

    public Result getAllData(String rowkey) throws Exception {
        return getAllData(rowkey, null, null);
    }

    //格式化输出
    public void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp:" + cell.getTimestamp() + " ");
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
        }
    }

    //批量查找数据
    public ResultScanner scanData(String startRow, String stopRow) throws Exception {
        checkTable();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);
//        for (Result result : resultScanner) {
//            showCell(result);
//        }
        scan = new Scan();
        return resultScanner;
    }

    public ResultScanner scanData() throws Exception {
        checkTable();
        //scan.setStartRow(Bytes.toBytes(startRow));
        //scan.setStopRow(Bytes.toBytes(stopRow));
        scan.setCaching(10);
        scan.setMaxResultSize(10);
        scan.setFilter(new PageFilter(10));
        ResultScanner resultScanner = table.getScanner(scan);
//        for (Result result : resultScanner) {
//            showCell(result);
//        }
        scan = new Scan();
        return resultScanner;
    }

    /*
     * 根据简历id查出rowkey
     * @param tableName
     * @param resumeId
     * @return
     * @throws IOException
     */
    public ResultScanner getDataByFilter(String tableName, String resumeId) throws Exception {
        checkTable();
        String family = "basic";
        String qual = "resume_id";
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(qual), CompareOp.EQUAL, Bytes.toBytes(resumeId));
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner rs = table.getScanner(scan);
        return rs;
    }

    public void busy() {
        isBusy = true;
    }

    public void free() {
        isBusy = false;
    }

    public boolean isbusy() {
        return isBusy;
    }

    public Table getTable() {
        return table;
    }
}
