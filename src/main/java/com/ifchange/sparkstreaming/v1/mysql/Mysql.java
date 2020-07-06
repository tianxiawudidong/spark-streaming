package com.ifchange.sparkstreaming.v1.mysql;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.sparkstreaming.v1.util.MyString;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * MYSQL数据库底层封装
 *
 * @author Administrator
 */
public class Mysql implements Serializable {

    private PreparedStatement pstmt;
    private Connection conn;
    private ResultSet rs;
    private volatile boolean isBusy = false;
    private String tableName = "mysql";
    private String dbUsername;
    private String passWord;
    private String dbhost = "127.0.0.1";
    private int dbport = 3306;
    private String enCoding = "utf-8";
    private static final Logger logger= Logger.getLogger(Mysql.class);

    public Mysql(String username, String password) throws SQLException {
        dbUsername = username;
        passWord = password;
        createConn();
    }

    public Mysql(String username, String password, String dbname) throws SQLException {
        tableName = dbname;
        dbUsername = username;
        passWord = password;
        createConn();
    }

    public Mysql(String username, String password, String dbname, String host) throws SQLException {
        tableName = dbname;
        dbUsername = username;
        dbhost = host;
        passWord = password;
        createConn();
    }

    public Mysql(String username, String password, String dbname, String host, int port) throws SQLException {
        tableName = dbname;
        dbUsername = username;
        dbhost = host;
        dbport = port;
        passWord = password;
        createConn();
    }

    public Mysql(String username, String password, String dbname, String host, int port, String encoding) throws SQLException {
        tableName = dbname;
        dbUsername = username;
        dbhost = host;
        dbport = port;
        passWord = password;
        enCoding = encoding;
        createConn();
    }

    public String getTableName() {
        return tableName;
    }

    public String getHost() {
        return dbhost;
    }

    private void createConn() throws SQLException {
        conn = DBConnection.getDBConnection(dbUsername, passWord, tableName, dbhost, dbport, enCoding);
    }

    /**
     * 执行修改添加操作
     *
     * @param coulmn
     * @param type
     * @param sql
     * @return
     * @throws SQLException
     */
    public boolean updateOrAdd(String[] coulmn, int[] type, String sql) throws SQLException {
        if (!setPstmtParam(coulmn, type, sql)) {
            return false;
        }
        boolean flag = pstmt.executeUpdate() > 0 ? true : false;
        close();
        return flag;
    }

    /**
     * 获取查询结果集
     *
     * @param coulmn
     * @param type
     * @param sql
     * @throws SQLException
     */
    public DataTable getResultData(String[] coulmn, int[] type, String sql) throws SQLException {
        DataTable dt = new DataTable();

        ArrayList<HashMap<String, String>> list = new ArrayList<HashMap<String, String>>();

        if (!setPstmtParam(coulmn, type, sql)) {
            return null;
        }
        rs = pstmt.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();//取数据库的列名 
        int numberOfColumns = rsmd.getColumnCount();
        while (rs.next()) {
            HashMap<String, String> rsTree = new HashMap<String, String>();
            for (int r = 1; r < numberOfColumns + 1; r++) {
                rsTree.put(rsmd.getColumnName(r), rs.getObject(r).toString());
            }
            list.add(rsTree);
        }
        close();
        dt.setDataTable(list);
        return dt;
    }

    /**
     * 参数设置
     *
     * @param coulmn
     * @param type
     * @throws SQLException
     * @throws NumberFormatException
     */
    private boolean setPstmtParam(String[] coulmn, int[] type, String sql) throws NumberFormatException, SQLException {
        if (sql == null) {
            return false;
        }
        pstmt = conn.prepareStatement(sql);
        if (coulmn != null && type != null && coulmn.length != 0 && type.length != 0) {
            for (int i = 0; i < type.length; i++) {
                switch (type[i]) {
                    case Types.INTEGER:
                        pstmt.setInt(i + 1, Integer.parseInt(coulmn[i]));
                        break;
                    case Types.SMALLINT:
                        pstmt.setInt(i + 1, Integer.parseInt(coulmn[i]));
                        break;
                    case Types.BOOLEAN:
                        pstmt.setBoolean(i + 1, Boolean.parseBoolean(coulmn[i]));
                        break;
                    case Types.CHAR:
                        pstmt.setString(i + 1, coulmn[i]);
                        break;
                    case Types.DOUBLE:
                        pstmt.setDouble(i + 1, Double.parseDouble(coulmn[i]));
                        break;
                    case Types.FLOAT:
                        pstmt.setFloat(i + 1, Float.parseFloat(coulmn[i]));
                        break;
                    case Types.BIGINT:
                        pstmt.setLong(i + 1, Long.parseLong(coulmn[i]));
                        break;
                    default:
                        break;
                }
            }
        }
        return true;
    }

    /**
     * 关闭数据库
     *
     * @throws SQLException
     */
    public void close() throws SQLException {
        if (rs != null) {
            rs.close();
            rs = null;
        }
        if (pstmt != null) {
            pstmt.close();
            pstmt = null;
        }
        if (conn != null) {
            conn.close();
        }
    }

    public Map<String, Object> executeQueryAd(String sql) throws SQLException {
        busy();
        Map<String, Object> list = new HashMap<String, Object>();
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    System.out.println("region_id:" + rs.getInt("region_id"));
                    System.out.println("id:" + rs.getLong("id"));
                    System.out.println("name:" + rs.getString("name"));
                    System.out.println("level:" + rs.getInt("level"));
                    list.put("id", rs.getLong("id"));
                    list.put("region_id", rs.getInt("region_id"));
                    list.put("name", rs.getString("name"));
                    list.put("level", rs.getInt("level"));
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return list;
    }

    public Map<String, Object> executeQueryRegion(String sql) throws SQLException {
        busy();
        Map<String, Object> list = new HashMap<String, Object>();
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    list.put("id", rs.getLong("id"));
                    list.put("level", rs.getInt("level"));
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return list;
    }


    public List<Map<String, Object>> executeQuery(String sql) throws SQLException {
        busy();
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                rs = pstmt.executeQuery();
                ResultSetMetaData rsmd = rs.getMetaData();//取数据库的列名 
                int numberOfColumns = rsmd.getColumnCount();
                while (rs.next()) {
                    HashMap<String, Object> rsTree = new HashMap<String, Object>();
                    for (int r = 1; r < numberOfColumns + 1; r++) {
                        rsTree.put(rsmd.getColumnLabel(r), rs.getObject(r));
                    }
                    list.add(rsTree);
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return list;
    }

    public List<Map<String, String>> executeQueryResume(String sql) throws SQLException {
        busy();
        List<Map<String, String>> list = new ArrayList<>();
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    HashMap<String, String> map = new HashMap<>();
                    long id = rs.getLong("id");
                    map.put("resume_id", String.valueOf(id));
                    Blob compress = rs.getBlob("compress");
                    if (null != compress) {
                        String resume = MyString.unzipString(compress.getBytes(1L, (int) (compress.length())));
                        map.put("resume_data", resume);
                    } else
                        map.put("resume_data", "");
                    String resume_updated_at = rs.getString("resume_updated_at");
                    if (StringUtils.isNotBlank(resume_updated_at)) {
                        map.put("resume_updated_at", resume_updated_at);
                    } else {
                        map.put("resume_updated_at", "");
                    }
                    Integer work_experience = rs.getInt("work_experience");
                    if (null != work_experience) {
                        map.put("work_experience", String.valueOf(work_experience));
                    } else {
                        map.put("work_experience", "0");
                    }
                    String arth = rs.getString("arth");
                    if (StringUtils.isNotBlank(arth))
                        map.put("arth", arth);
                    else
                        map.put("arth", "");
                    list.add(map);
                }
                free();
                break;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return list;
    }

    public Map<String, Object> executeQuerys(String sql) throws SQLException {
        Map<String, Object> map = new ConcurrentHashMap<>();
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    long id = rs.getLong("id");
                    map.put("resume_id", id);
                    Blob compress = rs.getBlob("compress");
                    if (null != compress) {
                        String resume = MyString.unzipString(compress.getBytes(1L, (int) (compress.length())));
                        map.put("resume_data", resume);
                    } else {
                        map.put("resume_data", "");
                    }
                    String resume_updated_at = rs.getString("resume_updated_at");
                    if (StringUtils.isNotBlank(resume_updated_at)) {
                        map.put("resume_updated_at", resume_updated_at);
                    } else {
                        map.put("resume_updated_at", "");
                    }
                    Integer work_experience = rs.getInt("work_experience");
                    if (null != work_experience) {
                        map.put("work_experience", String.valueOf(work_experience));
                    } else {
                        map.put("work_experience", "0");
                    }
                    String arth = rs.getString("arth");
                    if (StringUtils.isNotBlank(arth))
                        map.put("arth", arth);
                    else
                        map.put("arth", "");
                }
                free();
                break;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return map;
    }

    public Map<String, String> selectResume(String sql) throws SQLException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Map<String, String> map = new ConcurrentHashMap<String, String>();
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    long id = rs.getLong("id");
                    map.put("resume_id", String.valueOf(id));
                    /**
                     * update at 8/23
                     * 数据库compress改成16进制
                     */
                    String compress = rs.getString("compress");
                    if (StringUtils.isNotBlank(compress)) {
                        String resume = "";
                        try {
                            resume = MyString.unzipString(MyString.hexStringToBytes(compress));
                        } catch (IOException e) {
                            Blob compress2 = rs.getBlob("compress");
                            resume = MyString.unzipString(compress2.getBytes(1, (int) compress2.length()));
                        }
                        map.put("resume_data", resume);
                    } else {
                        map.put("resume_data", "");
                    }
                    Timestamp resume_updated_at = rs.getTimestamp("resume_updated_at");
                    map.put("resume_updated_at", null != resume_updated_at ? sdf.format(resume_updated_at) : "");
                    Integer work_experience = rs.getInt("work_experience");
                    if (null != work_experience) {
                        map.put("work_experience", String.valueOf(work_experience));
                    } else {
                        map.put("work_experience", "0");
                    }
                    String arth = rs.getString("arth");
                    if (StringUtils.isNotBlank(arth))
                        map.put("arth", arth);
                    else
                        map.put("arth", "");
                }
                free();
                break;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return map;
    }

    public String executeQueryCompress(String sql) throws SQLException {
        String compressStr = "";
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    /**
                     * 8/23
                     * 数据库resume_extras compress字段改成16进制字符串
                     */
                    String compress = rs.getString("compress");
                    if (StringUtils.isNotBlank(compress)) {
                        try {
                            compressStr = MyString.unzipString(MyString.hexStringToBytes(compress));
                        } catch (IOException e) {
                            Blob compress2 = rs.getBlob("compress");
                            compressStr = MyString.unzipString(compress2.getBytes(1, (int) compress2.length()));
                            break;
                        }
                    }
                }
                free();
                break;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return compressStr;
    }

    /**
     * 查询compress
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public String queryCompress(String sql) throws Exception {
        String compressStr = "";
        pstmt = conn.prepareStatement(sql);
        rs = pstmt.executeQuery();
        while (rs.next()) {
            try {
                String compress = rs.getString("compress");
                if (StringUtils.isNotBlank(compress)) {
                    byte[] bytes = MyString.hexStringToBytes(compress);
                    compressStr = MyString.unzipString(bytes);
                }
            } catch (Exception e) {
                Blob compress = rs.getBlob("compress");
                if (null != compress) {
                    compressStr = MyString.unzipString(compress.getBytes(1, (int) compress.length()));
                }
            }
        }
        free();
        return compressStr;
    }

    /**
     * 查询compress
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public List<Map<String, String>> executeQuerysCompress(String sql) throws SQLException {
        List<Map<String, String>> list = new ArrayList<>();
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    Map<String, String> map = new HashMap<>();
                    long id = rs.getLong("id");
                    map.put("id", String.valueOf(id));
                    /**
                     * 8/23
                     * 数据库resume_extras compress字段16进制字符串
                     */
                    String compress = rs.getString("compress");
//                    Blob compress = rs.getBlob("compress");
                    String compressStr = MyString.unzipString(MyString.hexStringToBytes(compress));
                    map.put("compress", compressStr);
                    list.add(map);
                }
                free();
                break;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return list;
    }


    public String executeQuerys2(String sql) throws SQLException {
        StringBuffer sb = new StringBuffer();
        sb.append("{");
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    long id = rs.getLong("id");
                    sb.append("\"resume_id\":" + id + "");
                    sb.append(",");
                    String compress = rs.getString("compress");
                    String resume = MyString.unzipString(MyString.hexStringToBytes(compress));
                    sb.append("\"resume_data\":\"" + resume + "\"");
                    sb.append(",");
                    String resume_updated_at = rs.getString("resume_updated_at");
                    if (StringUtils.isNotBlank(resume_updated_at)) {
                        sb.append("\"resume_updated_at\":\"" + resume_updated_at + "\"");
                    } else {
                        sb.append("\"resume_updated_at\":\"\"\"");
                    }
                }

                free();
                break;
            } catch (Exception ex) {
                sb = new StringBuffer();
                sb.append("{");
                sb.append("\"resume_id\":" + 0 + "");
                sb.append(",");
                sb.append("\"resume_data\":\"\"");
                sb.append("\"resume_updated_at\":\"\"\"");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * @param sql
     * @return
     * @throws SQLException
     */
    public List<String> executeQueryAll(String sql) throws SQLException {
        List<String> list = new ArrayList<String>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                Map<String, String> map = new HashMap<String, String>();
                String compress = rs.getString("compress");
                String resume_id = String.valueOf(rs.getInt("id"));
                String resume = MyString.unzipString(MyString.hexStringToBytes(compress));
                int workExperience = rs.getInt("work_experience");
                String cv_trade = rs.getString("cv_trade");
                String cv_title = rs.getString("cv_title");
                String cv_tag = rs.getString("cv_tag");
                String cv_entity = rs.getString("cv_entity");
                String cv_education = rs.getString("cv_education");
                String cv_feature = rs.getString("cv_feature");
                int cv_degree = rs.getInt("cv_degree");
                map.put("resume_data", resume);
                map.put("cv_entity", cv_entity);
                map.put("cv_functions", "");
                map.put("cv_tag", cv_tag);
                map.put("cv_education", cv_education);
                map.put("cv_title", cv_title);
                map.put("work_experience", String.valueOf(workExperience));
                map.put("cv_feature", cv_feature);
                map.put("cv_degree", String.valueOf(cv_degree));
                map.put("resume_id", resume_id);
                map.put("cv_trade", cv_trade);
                String str = JSON.toJSONString(map);
                list.add(str);
            }
            free();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return list;
    }

    public Map<String, Object> executeQueryResumeAndArth2(String sql) throws SQLException {
        Map<String, Object> map = new HashMap<>();
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    long id = rs.getLong("id");
                    map.put("resume_id", id);
                    String compress = rs.getString("compress");
                    if (null != compress) {
                        String resume = "";
                        try {
                            resume = MyString.unzipString(MyString.hexStringToBytes(compress));
                        } catch (Exception e) {
                            System.out.println(id + "的compress字段解压失败");
                            Blob compress1 = rs.getBlob("compress");
                            resume = MyString.unzipString(compress1.getBytes(1, (int) compress1.length()));
                        }
                        map.put("resume_data", resume);
                    } else {
                        map.put("resume_data", "");
                    }
                    Timestamp resume_updated_at = rs.getTimestamp("resume_updated_at");
                    map.put("resume_updated_at", resume_updated_at);
                    Integer work_experience = rs.getInt("work_experience");
                    if (null != work_experience) {
                        map.put("work_experience", String.valueOf(work_experience));
                    } else {
                        map.put("work_experience", 0);
                    }
                    String arth = rs.getString("arth");
                    if (StringUtils.isNotBlank(arth))
                        map.put("arth", arth);
                    else
                        map.put("arth", "");
                }
                free();
                break;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return map;
    }

    public int executeUpdate(String sql) throws SQLException {
        busy();
        int result = 0;
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                result = pstmt.executeUpdate();
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return result;
    }

    /**
     * insert 返回主键id
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public int executeInsert(String sql) throws SQLException {
        busy();
        int id = 0;
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                pstmt.executeUpdate();
                ResultSet res = pstmt.getGeneratedKeys();
                while (res.next()) {
                    id = res.getInt(1);
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return id;
    }

    /**
     * insert 返回主键id
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public int executeInsertByJson(String sql, JSONObject jsons, BigDecimal lng, BigDecimal lat, long addressId) throws SQLException {
        busy();
        int id = 0;
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                String stationName = jsons.getString("name");
                JSONObject location = jsons.getJSONObject("location");
                BigDecimal sLat = location.getBigDecimal("lat");
                BigDecimal sLng = location.getBigDecimal("lng");
                String sAddress = jsons.getString("address");
                int transportation = 0;
                JSONObject detailInfo = jsons.getJSONObject("detail_info");
                int distance = detailInfo.getInteger("distance");
                pstmt.setBigDecimal(1, lng);
                pstmt.setBigDecimal(2, lat);
                pstmt.setString(3, stationName);
                pstmt.setBigDecimal(4, sLng);
                pstmt.setBigDecimal(5, sLat);
                pstmt.setLong(6, addressId);
                pstmt.setInt(7, transportation);
                pstmt.setInt(8, distance);
                pstmt.setString(9, sAddress);
                pstmt.executeUpdate();
                ResultSet res = pstmt.getGeneratedKeys();
                while (res.next()) {
                    id = res.getInt(1);
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return id;
    }

    /**
     * 批量插入数据
     *
     * @param sql
     * @param strs
     * @param tid
     * @throws SQLException
     */
    public void executeBatchInsertByArray(String sql, String[] strs, int tid) throws SQLException {
        busy();
        while (true) {
            conn.setAutoCommit(false);
            try {
                pstmt = conn.prepareStatement(sql);
                for (String str : strs) {
                    pstmt.setInt(1, tid);
                    pstmt.setString(2, str);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
                conn.commit();
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
    }

    /**
     * insert into `echeng_log_request_interval_statistics`
     * (`f`,`work_name`,`from_time`,`end_time`,`avg_response_time`,`fail_rate`,`type_name`)
     * values(?,?,?,?,?,?,?)
     * @param sql
     * @param list
     * @param type
     * @param from
     * @param to
     * @throws SQLException
     */
    public void executeBatchInsertForLog(String sql, List<Tuple2<String, Tuple2<Tuple2<Integer, Double>, Integer>>> list,
                                         String type, String from, String to) throws SQLException {
        busy();
        while (true) {
            conn.setAutoCommit(false);
            try {
                pstmt = conn.prepareStatement(sql);
                for (Tuple2<String, Tuple2<Tuple2<Integer, Double>, Integer>> tuple : list) {
                    String key = tuple._1();
                    String[] split = key.split("\\+");
                    String f = split[0];
                    String w = split[1];
                    Tuple2<Tuple2<Integer, Double>, Integer> tuple2 = tuple._2();
                    int failCount = tuple2._1()._1();
                    double time = tuple2._1()._2();
                    int number = tuple2._2();
                    double avgTime = time / number;
                    double failRate = failCount / number;
                    String str=String.format("f=%1$s,w=%2$s,from=%3$s,to=%4$s,avgTime=%5$f,failRate=%6$f,typeName=%7$s",f, w, from, to, avgTime, failRate, type);
                    logger.info(str);
                    pstmt.setString(1, f);
                    pstmt.setString(2, w);
                    pstmt.setString(3, from);
                    pstmt.setString(4, to);
                    pstmt.setDouble(5, avgTime);
                    pstmt.setDouble(6, failRate);
                    pstmt.setString(7, type);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
                conn.commit();
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
    }


    /**
     * insert into `echeng_log_request_interval_statistics`
     * (`f`,`work_name`,`from_time`,`end_time`,`avg_response_time`,`type_name`)
     * values(?,?,?,?,?,?)
     *
     * @param sql
     * @param list
     * @param type
     * @param from
     * @param to
     * @throws SQLException
     */
    public void executeBatchInsertForTob1(String sql, List<Tuple2<String, Tuple2<Double, Integer>>> list,
                                         String type, String from, String to) throws SQLException {
        busy();
        while (true) {
            conn.setAutoCommit(false);
            try {
                pstmt = conn.prepareStatement(sql);
                for (Tuple2<String, Tuple2<Double, Integer>> tuple: list) {
                    String w = tuple._1();
                    String[] split = w.split("\\+");
                    String f = split[0];
                    String work = split[1];
                    Tuple2<Double, Integer> tuple2 = tuple._2();
                    Double time = tuple2._1();
                    int number = tuple2._2();
                    double avgTime = time / number;
                    pstmt.setString(1, f);
                    pstmt.setString(2, work);
                    pstmt.setString(3, from);
                    pstmt.setString(4, to);
                    pstmt.setDouble(5, avgTime);
                    pstmt.setString(6, type);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
                conn.commit();
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
    }

    /**
     * insert into `echeng_log_request_interval_statistics`
     * (`work_name`,`from_time`,`end_time`,`avg_response_time`,`fail_rate`,`type_name`)
     * values(?,?,?,?,?,?)
     * @param sql
     * @param list
     * @param type
     * @param from
     * @param to
     * @throws SQLException
     */
    public void executeBatchInsertForTob2(String sql, List<Tuple2<String, Tuple2<Tuple2<Integer, Double>, Integer>>> list,
                                          String type, String from, String to) throws SQLException {
        busy();
        while (true) {
            conn.setAutoCommit(false);
            try {
                pstmt = conn.prepareStatement(sql);
                for (Tuple2<String, Tuple2<Tuple2<Integer, Double>, Integer>> data : list) {
                    String w=data._1;
                    int failCount=data._2._1._1;
                    double totalTime=data._2._1._2;
                    int totalNum=data._2._2;
                    double avgTime=totalTime/totalNum;
                    double failRate=failCount/totalNum;
                    pstmt.setString(1, w);
                    pstmt.setString(2, from);
                    pstmt.setString(3, to);
                    pstmt.setDouble(4, avgTime);
                    pstmt.setDouble(5, failRate);
                    pstmt.setString(6, type);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
                conn.commit();
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
    }


    public boolean execute(String sql) throws SQLException {
        busy();
        boolean result = false;
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                result = pstmt.execute(sql);
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return result;
    }

    public List<String> listTable() throws SQLException {
        busy();
        List<String> tables = new ArrayList<String>();
        while (true) {
            try {
                pstmt = conn.prepareStatement(String.format("SHOW TABLES FROM `%s`", tableName));
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    tables.add(rs.getObject("Tables_in_".concat(tableName)).toString());
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return tables;
    }

    public List<String> listDatabase() throws SQLException {
        busy();
        List<String> databases = new ArrayList<String>();
        while (true) {
            try {
                pstmt = conn.prepareStatement("show databases");
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    databases.add(rs.getObject("Database").toString());
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return databases;
    }

    public void busy() {
        isBusy = true;
    }

    public void free() throws SQLException {
        isBusy = false;
        if (pstmt != null) {
            pstmt.close();
            pstmt = null;
        }
        if (rs != null) {
            rs.close();
            rs = null;
        }
    }

    public boolean isbusy() {
        return isBusy;
    }

    public void setTableName(String table) throws SQLException {
        tableName = table;
        execute("use `" + table + "`");
    }

    public boolean isValid() {
        try {
            return conn.isValid(3000);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean isSupportsBatchUpdates() throws SQLException {
        //conn是Connection的类型
        DatabaseMetaData dbmd = conn.getMetaData();
        //为true则意味着该数据是支持批量更新的
        return dbmd.supportsBatchUpdates();
    }

    public Connection getConn() {
        reConnect();
        return conn;
    }

    private void reConnect() {
        while (true) try {
            free();
            if (!conn.isValid(3000)) {
                close();
                createConn();
            }
            break;
        } catch (SQLException ex) {
            ex.printStackTrace();
            try {
                Thread.currentThread().sleep(1000);
            } catch (InterruptedException e) {
            }
        }
    }

    private boolean processException(SQLException ex) throws SQLException {
        int err_code = ex.getErrorCode();
        String err_msg = ex.getMessage();
        System.out.println("_._._._._._._._._._" + err_code + ":" + err_msg);
        if (!(err_code != 2013 && err_code != 2006 && err_code != 1053 && !err_msg.contains("No operations allowed after connection closed") && !err_msg.contains("The last packet successfully received from"))) {
            ex.printStackTrace();
            reConnect();
        } else {
            throw new SQLException(ex.getMessage(), ex.getSQLState(), err_code);
        }
        return true;
    }
}
