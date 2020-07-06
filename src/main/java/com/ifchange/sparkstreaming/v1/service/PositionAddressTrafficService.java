package com.ifchange.sparkstreaming.v1.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.sparkstreaming.v1.mysql.Mysql;
import com.ifchange.sparkstreaming.v1.util.FileUtil;
import com.ifchange.sparkstreaming.v1.util.HttpClientUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 地址解析逻辑修改：
 * 增加地铁字典表
 *
 * @author root
 */
public class PositionAddressTrafficService {


    private static final Logger logger = Logger.getLogger(PositionAddressTrafficService.class);
    private static final String path = "/opt/hadoop/position/position-invalid.log";

    /*
     * 职位地址解析--职位交通信息
     * @param values {"address":"奥体万达广场6层 艾森国际健身","corporation_id":2141732,"id":23038734}
     * @param redis  redis统计调用接口次数
     * @param mysql  gsystem_traffic mysql连接
     * @return String
     */
    @SuppressWarnings("unchecked")
    public static String parseAddress(String values, Mysql mysql) {
        List<Map<String, Object>> traffic = new ArrayList<>();
//        LocalDate now = LocalDate.now();
//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
//        String str = now.format(formatter);
//        String key = "position-address-" + str;
//        String failKey = "position-address-fail-" + str;
        String positionAddressInfoStr = "";
        Map<String, Object> sb = new HashMap<>();
        if (StringUtils.isNotBlank(values)) {
            logger.info("传来的参数为:" + values);
            //1、解析kafka传来的json字符串，得到address
            JSONObject json = JSONObject.parseObject(values);
            int corporationId = null != json.getInteger("corporation_id") ? json.getInteger("corporation_id") : 0;
            int positionId = null != json.getInteger("id") ? json.getInteger("id") : 0;
            String address = StringUtils.isNotBlank(json.getString("address")) ? json.getString("address") : "";
            address = address.replaceAll("\\s*", "");
            if (StringUtils.isNotBlank(address)) {
                sb.put("corporation_id", corporationId);
                sb.put("position_id", positionId);
                sb.put("address", address);
                //2、到地址字典表(address_dirctionary)中,根据address查询
                String sql = "select a.id,a.name,a.region_id,ra.`level` from `gsystem_traffic`.address_dictionary a left join `gsystem`.regions_area ra on a.region_id=ra.id where a.name='" + address + "'";
                logger.info("查询地址字典表、区域表{" + sql + "}");
                Map<String, Object> list = null;
                try {
                    list = mysql.executeQueryAd(sql);
                } catch (SQLException e) {
                    String msg = positionId + "\t" + address + "\t查询地址字典表、区域表报错\t" + e.getMessage();
                    logger.info(msg);
                }
                if (null != list && list.size() > 0) {
                    // 查到数据
                    long addressId = (long) list.get("id");
                    int regionId = (int) list.get("region_id");
                    int level = (int) list.get("level");
                    int cityId = 0;
//                    logger.info(regionId + "---->" + level);
                    switch (level) {
                        case 2:
                            cityId = regionId;
                            break;
                        case 3:
                            String sql_region = "select r.parent_id from `gsystem`.regions_area r where r.id=" + regionId;
                            List<Map<String, Object>> ra = null;
                            try {
                                ra = mysql.executeQuery(sql_region);
                            } catch (SQLException e) {
                                String msg = positionId + "\t" + address + "\t查询区域字典表报错\t" + e.getMessage();
                                logger.info(msg);
                            }
                            if (null != ra && ra.size() > 0) {
                                Map<String, Object> map = ra.get(0);
                                cityId = (int) map.get("parent_id");
                            }
                            break;
                        default:
                            break;
                    }
                    sb.put("region_id", regionId);
                    logger.info("地址字典表有该条地址信息，address_id=" + addressId);
                    //3、根据职位id判断职位地址表(position_traffic_address)中是否有该条数据
                    String posAddSql = "select * from `gsystem_traffic`.position_traffic_address where position_id=" + positionId;
                    logger.info("根据职位id查询职位地址表," + posAddSql);
                    List<Map<String, Object>> pta = null;
                    try {
                        pta = mysql.executeQuery(posAddSql);
                    } catch (SQLException e) {
                        String msg = positionId + "\t" + address + "\t查询职位地址表报错\t" + e.getMessage();
                        logger.info(msg);
                    }
                    Map<String, Object> lists = new HashMap<>();
                    if (null != pta && pta.size() > 0) {
                        lists = pta.get(0);
                    }
                    if (null != lists && lists.size() > 0) {
                        logger.info("职位地址表有该职位Id的信息");
                        //update
                        String updatePosAddSql = "update `gsystem_traffic`.position_traffic_address set address_id=" + addressId + ",corporation_id=" + corporationId + " where position_id=" + positionId;
                        logger.info("更新职位地址信息表:" + updatePosAddSql);
                        try {
                            mysql.execute(updatePosAddSql);
                        } catch (SQLException e) {
                            String msg = positionId + "\t更新职位地址表报错\t" + e.getMessage();
                            logger.info(msg);
                        }
                    } else {
                        logger.info("职位地址表没有该职位Id的信息");
                        //insert
                        String insertPosAddSql = "insert into `gsystem_traffic`.position_traffic_address(address_id,position_id,corporation_id) values(" + addressId + "," + positionId + "," + corporationId + ")";
                        logger.info("新增职位地址信息表:" + insertPosAddSql);
                        try {
                            mysql.execute(insertPosAddSql);
                        } catch (SQLException e) {
                            String msg = positionId + "\t插入职位地址表报错\t" + e.getMessage();
                            logger.info(msg);
                        }
                    }
                    //3.1、根据地址id，查询地址交通信息
                    String sql_add_tra = "select * from `gsystem_traffic`.`address_traffic`  where address_id=" + addressId;
                    logger.info("根据地址id，查询地址交通信息," + sql_add_tra);
                    List<Map<String, Object>> traffics = null;
                    try {
                        traffics = mysql.executeQuery(sql_add_tra);
                    } catch (SQLException e) {
                        String msg = addressId + "\t查询地址交通信息表报错\t" + e.getMessage();
                        logger.info(msg);
                    }
                    if (null != traffics && traffics.size() > 0) {
                        //该address_id下存在地铁交通信息\
                        logger.info("该地址id:" + addressId + ",下存在地铁交通信息");
                        BigDecimal cLng = (BigDecimal) traffics.get(0).get("c_lng");
                        BigDecimal cLat = (BigDecimal) traffics.get(0).get("c_lat");
                        sb.put("address_lng", cLng);
                        sb.put("address_lat", cLat);
                        for (Map<String, Object> map : traffics) {
                            Map<String, Object> traffic_map = new HashMap<>();
                            long tid = (long) map.get("id");
                            String addresses = (String) map.get("traffic_address");
                            traffic_map.put("station_name", map.get("station_name"));
                            traffic_map.put("s_lng", map.get("s_lng"));
                            traffic_map.put("s_lat", map.get("s_lat"));
                            traffic_map.put("transportation", map.get("transportation"));
                            traffic_map.put("distance", map.get("distance"));
                            List<Map<String, Object>> traffic_info = new ArrayList<>();
                            if (StringUtils.isNotBlank(addresses)) {
                                String[] sAddArray = addresses.split(";");
                                if (sAddArray.length > 0) {
                                    //到地铁线路别名表(metro_aliases)中，根据name，city_id去查出地铁id(metro_id)
                                    for (String names : sAddArray) {
                                        Map<String, Object> traffic_info_map = new HashMap<>();
                                        String metroAliasesSql = "select ma.* from `gsystem_traffic`.metro_aliases ma  where ma.name='" + names + "' and ma.city_id=" + cityId;
                                        logger.info("查询地铁线路别名表：" + metroAliasesSql);
                                        List<Map<String, Object>> ma = null;
                                        try {
                                            ma = mysql.executeQuery(metroAliasesSql);
                                        } catch (SQLException e) {
                                            String msg = positionId + "\t查询地铁线路别名表报错\t" + e.getMessage();
                                            logger.info(msg);
                                        }
                                        Map<String, Object> mas = new HashMap<>();
                                        if (null != ma && ma.size() > 0) {
                                            mas = ma.get(0);
                                        }
                                        if (null != mas && mas.size() > 0) {
                                            //地铁线路别名中存在数据
                                            //将信息写入交通站点地铁关联表（traffic_metros）
                                            int metro_id = (int) mas.get("metro_id");
                                            traffic_info_map.put("tid", tid);
                                            traffic_info_map.put("metro_id", metro_id);
                                            traffic_info.add(traffic_info_map);
                                        }
                                    }
                                }
                            }
                            traffic_map.put("traffic_info", traffic_info);
                            traffic.add(traffic_map);
                        }
                        sb.put("traffic", traffic);
                    } else {
                        logger.info("该地址没有查到相关的交通信息,sql:[" + sql_add_tra + "]");
                        logger.info("...请求百度第一、第三个接口...");
                        // 调第一个接口 根据地址 获取经纬度
                        String url = "http://api.map.baidu.com/geocoder/v2/?address=" + address + "&output=json&ak=bAF9v1ZluIZzI1mT4BmYkPlKGG7ZUN67";
                        logger.info("first url" + url);
                        JSONObject res = null;
                        try {
                            res = HttpClientUtil.httpGet(url);
                        } catch (Exception e) {
                            String msg = positionId + "\t" + address + "\t请求百度接口1报错";
                            logger.info(msg);
                        }
                        if (null != res && res.size() > 0) {
                            logger.info("first url return:" + res);
                            JSONObject result1 = res.getJSONObject("result");
                            if (null != result1 && result1.size() > 0) {
                                JSONObject location = result1.getJSONObject("location");
                                BigDecimal lat = location.getBigDecimal("lat");
                                BigDecimal lng = location.getBigDecimal("lng");
                                sb.put("address_lng", lng);
                                sb.put("address_lat", lat);
                                sb.put("region_id", regionId);
                                // 调第三个接口 根据经纬度 获取周围地铁数据
                                String url3 = "http://api.map.baidu.com/place/v2/search?" + "location=" + lat + "," + lng + "&output=json" + "&ak=bAF9v1ZluIZzI1mT4BmYkPlKGG7ZUN67" + "&radius=1000" + "&query=地铁" + "&page_size=50" + "&page_num=0" + "&scope=2";
                                logger.info("third url" + url3);
                                JSONObject res3 = null;
                                try {
                                    res3 = HttpClientUtil.httpGet(url3);
                                } catch (Exception e) {
                                    String msg = positionId + "\t" + address + "\t请求百度接口3报错";
                                    logger.info(msg);
                                }
                                if (null != res3 && res3.size() > 0) {
                                    logger.info("third url return:" + res3);
                                    JSONArray resultsJsonArray = res3.getJSONArray("results");
                                    // 把查出的地铁信息写入地址交通信息表address_traffic
                                    String insertAddTrasql = "insert into `gsystem_traffic`.address_traffic(`c_lng`,`c_lat`,`station_name`,`s_lng`,`s_lat`,`address_id`,`transportation`,`distance`,traffic_address)values(?,?,?,?,?,?,?,?,?)";
                                    if (null != resultsJsonArray && resultsJsonArray.size() > 0) {
                                        for (int i = 0; i < resultsJsonArray.size(); i++) {
                                            Map<String, Object> traffic_map = new HashMap<>();
                                            JSONObject jsons = resultsJsonArray.getJSONObject(i);
                                            if (null != jsons) {
                                                String sAddress = StringUtils.isNotBlank(jsons.getString("address")) ? jsons.getString("address") : "";
                                                if (StringUtils.isNotBlank(sAddress)) {
                                                    String[] sAddArray = sAddress.split(";");
                                                    // 地址交通信息主键traffic_id
                                                    String stationName = jsons.getString("name");
                                                    JSONObject location2 = jsons.getJSONObject("location");
                                                    BigDecimal sLat = location2.getBigDecimal("lat");
                                                    BigDecimal sLng = location2.getBigDecimal("lng");
                                                    int transportation = 0;
                                                    JSONObject detailInfo = jsons.getJSONObject("detail_info");
                                                    int distance = detailInfo.getInteger("distance");
                                                    int tid = 0;
                                                    try {
                                                        tid = mysql.executeInsertByJson(insertAddTrasql, jsons, lng, lat, addressId);
                                                    } catch (SQLException e) {
                                                        String msg = positionId + "\t" + address + "\t写入地址交通信息表报错\t" + e.getMessage();
                                                        logger.info(msg);
                                                    }
                                                    traffic_map.put("station_name", stationName);
                                                    traffic_map.put("s_lng", sLng);
                                                    traffic_map.put("s_lat", sLat);
                                                    traffic_map.put("transportation", transportation);
                                                    traffic_map.put("distance", distance);
                                                    List<Map<String, Object>> traffic_info = new ArrayList<>();
                                                    //到地铁线路别名表(metro_aliases)中，根据name，city_id去查出metro_id
                                                    for (String names : sAddArray) {
                                                        Map<String, Object> traffic_info_map = new HashMap<>();
                                                        String metroAliasesSql = "select ma.* from `gsystem_traffic`.metro_aliases ma where ma.name='" + names + "' and ma.city_id=" + cityId;
                                                        logger.info("查询地铁线路别名表：" + metroAliasesSql);
                                                        List<Map<String, Object>> ma = null;
                                                        try {
                                                            ma = mysql.executeQuery(metroAliasesSql);
                                                        } catch (SQLException e) {
                                                            String msg = positionId + "\t" + address + "\t查询地铁线路别名表报错\t" + e.getMessage();
                                                            logger.info(msg);
                                                        }
                                                        Map<String, Object> mas = new HashMap<>();
                                                        if (null != ma && ma.size() > 0) {
                                                            mas = ma.get(0);
                                                        }
                                                        if (null != mas && mas.size() > 0) {
                                                            //地铁线路别名中存在数据
                                                            //将信息写入交通站点地铁关联表（traffic_metros）
                                                            int metro_id = (int) mas.get("metro_id");
                                                            String insertTrafficMetrosSql = "insert into `gsystem_traffic`.traffic_metros(`traffic_id`,`metro_id`) values(" + tid + "," + metro_id + ") ";
                                                            try {
                                                                mysql.execute(insertTrafficMetrosSql);
                                                            } catch (SQLException e) {
                                                                String msg = positionId + "\t" + address + "\t写入交通站点地铁关联表报错\t" + e.getMessage();
                                                                logger.info(msg);
                                                            }
                                                            traffic_info_map.put("tid", tid);
                                                            traffic_info_map.put("metro_id", metro_id);
                                                            traffic_info.add(traffic_info_map);
                                                        } else {
                                                            //地铁线路别名中没有该数据
                                                            //将信息写入地铁线路临时表（traffic_metros_tmp）
                                                            String insertTrafficMetrosTmp = "insert into `gsystem_traffic`.traffic_metros_tmp(`name`,`traffic_id`,`city_id`) values('" + names + "'," + tid + "," + cityId + ") ";
                                                            logger.info("insert traffic_metros_tmp sql:{" + insertTrafficMetrosTmp + "}");
                                                            try {
                                                                mysql.execute(insertTrafficMetrosTmp);
                                                            } catch (SQLException e) {
                                                                String msg = positionId + "\t" + address + "\t写入地铁线路临时表报错\t" + e.getMessage();
                                                                logger.info(msg);
                                                            }
                                                        }
                                                    }
                                                    traffic_map.put("traffic_info", traffic_info);
                                                }
                                            }
                                            traffic.add(traffic_map);
                                        }
                                    }
                                }
                                sb.put("traffic", traffic);
                            } else {
                                logger.info("first url not get data,positionId:" + positionId + "address:" + address);
                                /*
                                 * redis --统计调百度接口没有返回数据的数量
                                 */
                                String msg = positionId + "\t" + address + "\t百度接口1没有查到数据";
                                logger.info(msg);
                                sb.put("address_lng", 0);
                                sb.put("address_lat", 0);
                                sb.put("traffic", traffic);
                            }
                        }
                    }
                } else {
                    // 地址字典表中不存在该地址
                    //4、去请求百度接口
                    // 调第一个接口 根据地址 获取经纬度
                    String url = "http://api.map.baidu.com/geocoder/v2/?address=" + address + "&output=json&ak=bAF9v1ZluIZzI1mT4BmYkPlKGG7ZUN67";
                    logger.info("first url" + url);
                    JSONObject res = null;
                    try {
                        res = HttpClientUtil.httpGet(url);
                    } catch (Exception e) {
                        String msg = positionId + "\t" + address + "\t请求百度接口1报错";
                        logger.info(msg);
                    }
                    if (null != res && res.size() > 0) {
                        logger.info("first url return:" + res);
                        JSONObject result1 = res.getJSONObject("result");
                        if (null != result1 && result1.size() > 0) {
                            JSONObject local = result1.getJSONObject("location");
                            BigDecimal lat = local.getBigDecimal("lat");
                            BigDecimal lng = local.getBigDecimal("lng");
                            sb.put("address_lng", lng);
                            sb.put("address_lat", lat);
                            // 调第二个接口 根据经纬度 获取省 市 区
                            String url2 = "http://api.map.baidu.com/geocoder/v2/?location=" + lat + "," + lng + "&output=json&ak=bAF9v1ZluIZzI1mT4BmYkPlKGG7ZUN67&pois=0";
                            logger.info("second url" + url2);
                            JSONObject res2 = null;
                            try {
                                res2 = HttpClientUtil.httpGet(url2);
                            } catch (Exception e) {
                                String msg = positionId + "\t" + address + "\t请求百度接口2报错";
                                logger.info(msg);
                            }
                            if (null != res2 && res2.size() > 0) {
                                JSONObject result2 = res2.getJSONObject("result");
                                if (null != result2 && result2.size() > 0) {
                                    JSONObject addressComponent = result2.getJSONObject("addressComponent");
                                    String province = null != addressComponent && addressComponent.size() > 0 ? addressComponent.getString("province") : "";
                                    String city = null != addressComponent && addressComponent.size() > 0 ? addressComponent.getString("city") : "";
                                    String district = null != addressComponent && addressComponent.size() > 0 ? addressComponent.getString("district") : "";
                                    // 调第三个接口 根据经纬度 获取周围地铁数据
                                    String url3 = "http://api.map.baidu.com/place/v2/search?" + "location=" + lat + "," + lng + "&output=json" + "&ak=bAF9v1ZluIZzI1mT4BmYkPlKGG7ZUN67" + "&radius=1000" + "&query=地铁" + "&page_size=50" + "&page_num=0" + "&scope=2";
                                    logger.info("third url" + url3);
                                    JSONObject res3 = null;
                                    try {
                                        res3 = HttpClientUtil.httpGet(url3);
                                    } catch (Exception e) {
                                        String msg = positionId + "\t" + address + "\t请求百度接口3报错";
                                        logger.info(msg);
                                    }
                                    // 根据省市区到regions_area表中 去查出regionId
                                    // 省
                                    String regionSql = "";
                                    if (StringUtils.isNotBlank(province) && StringUtils.isBlank(city) && StringUtils.isBlank(district)) {
                                        province = province.replace("省", "");
                                        province = province.replace("市", "");
                                        regionSql = "select id,level from `gsystem`.regions_area where name like '" + province + "%' and level=1 and id >1000000";
                                    } else if (StringUtils.isNotBlank(province) && StringUtils.isNotBlank(city) && StringUtils.isBlank(district)) {// 省+市
                                        city = city.replace("市", "");
                                        regionSql = "select id,level from `gsystem`.regions_area where name like '" + city + "%' and level=2 and id >1000000";
                                    } else if (StringUtils.isNotBlank(province) && StringUtils.isNotBlank(city) && StringUtils.isNotBlank(district)) {// 省+市+区
                                        city = city.replace("市", "");
                                        district = district.replace("区", "");
                                        district = district.replace("市", "");
                                        district = district.replace("县", "");
                                        regionSql = "select a.id,a.level from `gsystem`.regions_area a left join `gsystem`.regions_area f on a.parent_id=f.id  where a.name like '" + district + "%' and f.name like '" + city + "%' and a.level=3 and a.id >1000000";
                                    } else {// 省市区都为空 不做任何处理
                                        sb.put("status", 0);
                                        sb.put("traffic", traffic);
//                                        String message = positionId + "\t" + address + "\t" + "无效地址，查询不到regionId";
//                                        FileUtil.writeFile(path, message);
                                    }
                                    Map<String, Object> result = null;
                                    try {
                                        logger.info(regionSql);
                                        result = mysql.executeQueryRegion(regionSql);
                                        /*
                                         * update at 8/24
                                         * 百度接口2 查出省市区 然后查regions_area 查regionId
                                         * 1、先根据省市区查 如果没查到
                                         * 2、根据省市查
                                         * 3、根据省查
                                         */
                                        if (result.size() == 0) {
                                            logger.info("根据省市区查询regionId，没查出结果,根据省市查");
                                            city = city.replace("市", "");
                                            String regionSqlProAndCity = "select id,level from `gsystem`.regions_area where name like '" + city + "%' and level=2 and id >1000000";
                                            logger.info(regionSqlProAndCity);
                                            result = mysql.executeQueryRegion(regionSqlProAndCity);
                                            if (result.size() == 0) {
                                                logger.info("根据省市查询regionId,没查出结果,根据省查");
                                                province = province.replace("省", "");
                                                province = province.replace("市", "");
                                                String regionSqlPro = "select id,level from `gsystem`.regions_area where name like '" + province + "%' and level=1 and id >1000000";
                                                logger.info(regionSqlPro);
                                                result = mysql.executeQueryRegion(regionSqlPro);
                                            }
                                        }
                                    } catch (SQLException e) {
                                        String msg = positionId + "\t" + address + "\t查询区域字典表报错\t" + e.getMessage();
                                        logger.info(msg);
                                    }
                                    // 把查出的region_id写入地址字典表（address_dictionary）
                                    if (null != result && result.size() > 0) {
                                        long id = (long) result.get("id");
                                        //0 1 2 3
                                        int level = (int) result.get("level");
                                        long cityId = 0;
                                        logger.info(id + "---->" + level);
                                        switch (level) {
                                            case 2:
                                                cityId = id;
                                                break;
                                            case 3:
                                                String sqls = "select r.parent_id from `gsystem`.regions_area r where r.id=" + id;
                                                logger.info("查询父id:{" + sqls + "}");
                                                List<Map<String, Object>> executeQuery = null;
                                                try {
                                                    executeQuery = mysql.executeQuery(sqls);
                                                } catch (SQLException e) {
                                                    String msg = positionId + "\t" + address + "\t查询区域字典表报错";
                                                    logger.info(msg);
                                                }
                                                if (null != executeQuery && executeQuery.size() > 0) {
                                                    Map<String, Object> map = executeQuery.get(0);
                                                    cityId = (int) map.get("parent_id");
                                                }
                                                break;
                                            default:
                                                break;
                                        }
                                        sb.put("region_id", id);
                                        String insertAddSql = "insert into `gsystem_traffic`.address_dictionary(name,region_id)values('" + address + "'," + id + ")";
                                        logger.info("地址字典表===" + insertAddSql);
                                        int addressId = 0;
                                        try {
                                            addressId = mysql.executeInsert(insertAddSql);
                                        } catch (SQLException e) {
                                            String msg = positionId + "\t" + address + "\t写入地址字典表报错";
                                            logger.info(msg);
                                        }
                                        logger.info("addressId=====" + addressId);
                                        //判断职位地址表中(position_traffic_address)是否有该条数据
                                        String posAddSql = "select * from  `gsystem_traffic`.position_traffic_address where position_id=" + positionId;
                                        List<Map<String, Object>> lists = null;
                                        try {
                                            lists = mysql.executeQuery(posAddSql);
                                        } catch (SQLException e) {
                                            String msg = positionId + "\t" + address + "\t" + "\t查询职位地址表报错";
                                            logger.info(msg);
                                        }
                                        if (null != lists && lists.size() > 0) {
                                            //update
                                            String updatePosAddSql = "update `gsystem_traffic`.position_traffic_address set address_id=" + addressId + ",corporation_id=" + corporationId + " where position_id=" + positionId;
                                            logger.info("更新职位地址信息表:{" + updatePosAddSql + "}");
                                            try {
                                                mysql.execute(updatePosAddSql);
                                            } catch (SQLException e) {
                                                String msg = positionId + "\t" + address + "\t更新职位地址信息表报错";
                                                logger.info(msg);
                                            }
                                        } else {
                                            //insert
                                            String insertPosAddSql = "insert into `gsystem_traffic`.position_traffic_address(address_id,position_id,corporation_id) values(" + addressId + "," + positionId + "," + corporationId + ")";
                                            logger.info("新增职位地址信息表:{" + insertPosAddSql + "}");
                                            try {
                                                mysql.execute(insertPosAddSql);
                                            } catch (SQLException e) {
                                                String msg = positionId + "\t" + address + "\t新增职位地址信息表报错";
                                                logger.info(msg);
                                            }
                                        }
                                        // 把查出的地铁信息写入地址交通信息表address_traffic
                                        if (null != res3 && res3.size() > 0) {
                                            JSONArray resultsJsonArray = res3.getJSONArray("results");
                                            String insertAddTrasql = "insert into `gsystem_traffic`.address_traffic(`c_lng`,`c_lat`,`station_name`,`s_lng`,`s_lat`,`address_id`,`transportation`,`distance`,traffic_address)values(?,?,?,?,?,?,?,?,?)";
                                            if (null != resultsJsonArray && resultsJsonArray.size() > 0) {
                                                for (int i = 0; i < resultsJsonArray.size(); i++) {
                                                    Map<String, Object> traffic_map = new HashMap<>();
                                                    JSONObject jsons = resultsJsonArray.getJSONObject(i);
                                                    if (null != jsons) {
                                                        String sAddress = StringUtils.isNotBlank(jsons.getString("address")) ? jsons.getString("address") : "";
                                                        if (StringUtils.isNotBlank(sAddress)) {
                                                            String[] sAddArray = sAddress.split(";");
                                                            // 地址交通信息主键traffic_id
                                                            String stationName = jsons.getString("name");
                                                            JSONObject location = jsons.getJSONObject("location");
                                                            BigDecimal sLat = location.getBigDecimal("lat");
                                                            BigDecimal sLng = location.getBigDecimal("lng");
                                                            int transportation = 0;
                                                            JSONObject detailInfo = jsons.getJSONObject("detail_info");
                                                            int distance = detailInfo.getInteger("distance");
                                                            int tid = 0;
                                                            try {
                                                                tid = mysql.executeInsertByJson(insertAddTrasql, jsons, lng, lat, addressId);
                                                            } catch (SQLException e) {
                                                                String msg = positionId + "\t" + address + "\t写入地址交通信息表报错";
                                                                logger.info(msg);
                                                            }
                                                            traffic_map.put("station_name", stationName);
                                                            traffic_map.put("s_lng", sLng);
                                                            traffic_map.put("s_lat", sLat);
                                                            traffic_map.put("transportation", transportation);
                                                            traffic_map.put("distance", distance);
                                                            List<Map<String, Object>> traffic_info = new ArrayList<>();
                                                            //到地铁线路别名表(metro_aliases)中，根据name，city_id去查出metro_id
                                                            //注：地铁线路别名表中的city_id的层级都是2
                                                            for (String names : sAddArray) {
                                                                Map<String, Object> traffic_info_map = new HashMap<>();
                                                                String metroAliasesSql = "select * from `gsystem_traffic`.metro_aliases where name='" + names + "' and city_id=" + cityId;
                                                                logger.info("查询地铁线路别名表：" + metroAliasesSql);
                                                                List<Map<String, Object>> ma = null;
                                                                try {
                                                                    ma = mysql.executeQuery(metroAliasesSql);
                                                                } catch (SQLException e) {
                                                                    String msg = positionId + "\t" + address + "\t查询地铁线路别名表报错";
                                                                    logger.info(msg);
                                                                }
                                                                Map<String, Object> mas = new HashMap<>();
                                                                if (null != ma && ma.size() > 0) {
                                                                    mas = ma.get(0);
                                                                }
                                                                if (null != mas && mas.size() > 0) {
                                                                    //地铁线路别名中存在数据
                                                                    //将信息写入交通站点地铁关联表（traffic_metros）
                                                                    int metro_id = (int) mas.get("metro_id");
                                                                    String insertTrafficMetrosSql = "insert into `gsystem_traffic`.traffic_metros(`traffic_id`,`metro_id`) values(" + tid + "," + metro_id + ") ";
                                                                    logger.info("insert traffic_metros sql:{" + insertTrafficMetrosSql + "}");
                                                                    try {
                                                                        mysql.execute(insertTrafficMetrosSql);
                                                                    } catch (SQLException e) {
                                                                        String msg = positionId + "\t" + address + "\t写入交通站点地铁关联表报错";
                                                                        logger.info(msg);
                                                                    }
                                                                    traffic_info_map.put("tid", tid);
                                                                    traffic_info_map.put("metro_id", metro_id);
                                                                    traffic_info.add(traffic_info_map);
                                                                } else {
                                                                    //地铁线路别名中没有该数据
                                                                    //将信息写入地铁线路临时表（traffic_metros_tmp）
                                                                    String insertTrafficMetrosTmp = "insert into `gsystem_traffic`.traffic_metros_tmp(`name`,`traffic_id`,`city_id`) values('" + names + "'," + tid + "," + cityId + ") ";
                                                                    logger.info("insert traffic_metros_tmp sql:{" + insertTrafficMetrosTmp + "}");
                                                                    try {
                                                                        mysql.execute(insertTrafficMetrosTmp);
                                                                    } catch (SQLException e) {
                                                                        String msg = positionId + "\t" + address + "\t写入地铁线路临时表报错";
                                                                        logger.info(msg);
                                                                    }
                                                                }
                                                            }
                                                            traffic_map.put("traffic_info", traffic_info);
                                                        }
                                                    }
                                                    traffic.add(traffic_map);
                                                }
                                            }
                                            sb.put("traffic", traffic);
                                        } else {
                                            logger.info("third url not get data,positionId:" + positionId + "address:" + address);
                                            sb.put("traffic", traffic);
                                        }
                                    } else {
                                        logger.info(regionSql + ",没有查到region数据...");
                                        String message = positionId + "\t" + address + "\t" + "无效地址，查询不到regionId";
                                        FileUtil.writeFile(path, message);
                                        sb.put("traffic", traffic);
                                    }
                                } else {
                                    logger.info("second url not get data,positionId:" + positionId + "address:" + address);
                                    String message = positionId + "\t" + address + "\t" + "无效地址，查询不到regionId";
                                    FileUtil.writeFile(path, message);
                                    sb.put("traffic", traffic);
                                }
                            } else {
                                logger.info("second url res2 is null");
                                String message = positionId + "\t" + address + "\t" + "无效地址，查询不到regionId";
                                FileUtil.writeFile(path, message);
                                sb.put("traffic", traffic);
                            }
                        } else {
                            logger.info("first url not get data,positionId:" + positionId + "address:" + address);
                            String msg = positionId + "\t" + address + "\t百度接口1没有查到数据";
                            logger.info(msg);
                            sb.put("address_lng", 0);
                            sb.put("address_lat", 0);
                            sb.put("traffic", traffic);
                        }
                    } else {
                        logger.info("first url res is null");
                        String message = positionId + "\t" + address + "\t" + "无效地址，查询不到regionId";
                        FileUtil.writeFile(path, message);
                        sb.put("address_lng", 0);
                        sb.put("address_lat", 0);
                        sb.put("traffic", traffic);
                    }
                }
                positionAddressInfoStr = JSON.toJSONString(sb);
            }
        }
        return positionAddressInfoStr;
    }


    public static void main(String[] args) {
//        String dbName1 = "position_0";
//        String dbName2 = "position_1";
//        String dbName="gsystem_traffic";
//        String HOST1 = "192.168.8.130";
//        String HOST2 = "192.168.8.132";
//        MysqlPool mysqlPool1 = new MysqlPool(USERNAME, PASS, HOST1, PORT, dbName1);
//        MysqlPool mysqlPool2 = new MysqlPool(USERNAME, PASS, HOST2, PORT, dbName2);
//        String host = "192.168.8.101";
//        String database=args[0];
//        String id=args[1];
//        int num = Integer.parseInt(database.split("_")[1]);
//        Mysql mysql = num % 2 == 0 ? mysqlPool1.getMysqlConn() : mysqlPool2.getMysqlConn();
//        Mysql mysqlGsy = new Mysql(USERNAME, PASS, dbName, host, PORT);
//        String sql="select pe.address,pe.id,p.corporation_id from `"+database+"`.positions_extras pe left join `"+database+"`.positions p on pe.id=p.id where pe.id="+id;
//        List<Map<String, Object>> list = mysql.executeQuery(sql);
//        if (null != list && list.size() > 0) {
//            Map<String, Object> map = list.get(0);
//            String value = JSON.toJSONString(map);
//            logger.info(value);
//            String result = PositionAddressTrafficService.parseAddress(value, null, mysqlGsy);
//            logger.info(result);
//        }
//        Mysql mysqlGsy = new Mysql("devuser", "devuser", "gsystem_traffic", "192.168.1.201", 3306);
//        String msg = "{\"address\":\"浙江绍兴柯桥钱清\",\"corporation_id\":1246937,\"id\":2349448}";

    }

}
