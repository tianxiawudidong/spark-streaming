package com.ifchange.sparkstreaming.v1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.sparkstreaming.v1.gm.PositionAddressGearman;
import com.ifchange.sparkstreaming.v1.mysql.Mysql;
import com.ifchange.sparkstreaming.v1.mysql.MysqlPool;
import com.ifchange.sparkstreaming.v1.service.PositionAddressTrafficService;
import kafka.serializer.StringDecoder;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.*;
import java.util.concurrent.Future;

/**
 * spark streaming window
 * Created by Administrator on 2017/8/4.
 */
public class SparkStreamingForPositionAddress {

    private static final Logger logger = Logger.getLogger(SparkStreamingForPositionAddress.class);
    private static final String USERNAME = "bi";
    private static final String PASS = "Vuf6m91PRGz8G.F*GJA0";
    private static final String HOST1 = "192.168.8.85";
    private static final String HOST2 = "192.168.8.87";
    private static final String HOST = "192.168.8.101";
    private static final int PORT = 3307;
    private static MysqlPool mysqlPool1;
    private static MysqlPool mysqlPool2;
    private static MysqlPool mysqlPool;

    static {
        try {
            String dbName1 = "position_0";
            String dbName2 = "position_1";
            String dbName = "gsystem_traffic";
            mysqlPool1 = new MysqlPool(USERNAME, PASS, HOST1, PORT, dbName1);
            mysqlPool2 = new MysqlPool(USERNAME, PASS, HOST2, PORT, dbName2);
            mysqlPool = new MysqlPool(USERNAME, PASS, HOST, PORT, dbName);
        } catch (Exception e) {
            logger.info("init mysql pool error" + e.getMessage());
        }
        try {
            PositionAddressGearman.init(2, "icdc_position_save", "icdc_position_address");
        } catch (Exception e) {
            logger.info("初始化gearMan报错," + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {

        String master = args[0];
        String appName = "spark-streaming-position-address";
        String groupId = args[1];
        String topic = args[2];
        SparkConf conf = new SparkConf();
        conf.setAppName(appName);
        conf.setMaster(master);
        conf.set("app.log.name", "spark-streaming-position-address");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.minutes(1));
        jssc.checkpoint("/position-address/position-address-checks");

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "hadoop105:9092,hadoop107:9092,hadoop108:9092");
        // 有smallest、largest、anything可选，分别表示给当前最小的offset、当前最大的offset、抛异常。默认largest
        kafkaParams.put("auto.offset.reset", "largest");
        kafkaParams.put("zookeeper.connect", "hadoop105:2181,hadoop107:2181,hadoop108:2181");
        kafkaParams.put("group.id", groupId);

        Set<String> set = new HashSet();
        set.add(topic);
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, set);

//        lines.persist(StorageLevel.MEMORY_AND_DISK_SER());
        //kafka value
        //{"database":"position_5","table":"positions_extras","ts":"2017-08-04 13:59:35","type":"update","data":{"id":"25506245"}}
        JavaDStream<Integer> result = lines.map(rdd -> {
            String kafkaValue = rdd._2();
            logger.info(kafkaValue);
            JSONObject json = JSONObject.parseObject(kafkaValue);
            String database = json.getString("database");
            int num = Integer.parseInt(database.split("_")[1]);

            Mysql mysql = num % 2 == 0 ? mysqlPool1.getMysqlConn() : mysqlPool2.getMysqlConn();
            JSONObject dataJson = json.getJSONObject("data");
            String id = dataJson.getString("id");
            String sql = "select pe.address,pe.id,p.corporation_id from `" + database + "`.positions_extras pe left join `" + database + "`.positions p on pe.id=p.id where pe.id=" + id;
            logger.info("查询职位地址公司相关信息:" + sql);
            List<Map<String, Object>> list = mysql.executeQuery(sql);
            String str = "";
            if (null != list && list.size() > 0) {
                Map<String, Object> map = list.get(0);
                str = JSON.toJSONString(map);
            }
            logger.info("查询出的结果:" + str);
            if (num % 2 == 0) {
                mysqlPool1.free(mysql);
            } else {
                mysqlPool2.free(mysql);
            }
            String parseAddress ="";
            if (StringUtils.isNotBlank(str)) {
                Mysql mysqlGsy = mysqlPool.getMysqlConn();
                parseAddress = PositionAddressTrafficService.parseAddress(str, mysqlGsy);
                mysqlPool.free(mysqlGsy);
            }
            return parseAddress;
        }).map(parse -> {
            JSONObject parResult = null;
            int positionId = 0;
            if (StringUtils.isNotBlank(parse)) {
                try {
                    parResult = JSON.parseObject(parse);
                } catch (Exception e) {
                    logger.info("parse:[" + parse + "]，不能转成json");
                }
                if (null != parResult && parResult.size() > 0) {
                    positionId = parResult.getInteger("position_id");
                    JSONArray traffics = parResult.getJSONArray("traffic");
                    //调用通知接口
                    Map<String, Object> positionMap = new HashMap<>();
                    positionMap.put("position_id", positionId);
                    if (null != traffics && traffics.size() > 0) {
                        int trafficConvenient = 0;
                    /*
                      8/22
                      交通便利度逻辑修改--distance《=1000 traffic_convenient->1
                     */
                        for (int i = 0; i < traffics.size(); i++) {
                            JSONObject trafficInfo = traffics.getJSONObject(i);
                            if (null != trafficInfo) {
                                int distance = trafficInfo.getInteger("distance");
                                if (distance <= 1000) {
                                    trafficConvenient = 1;
                                    break;
                                }
                            }
                        }
                        parResult.put("traffic_convenient", trafficConvenient);
                    } else {
                        parResult.put("traffic_convenient", 0);
                    }
                    String jdAddress = parResult.toJSONString();
                    positionMap.put("flag_updated", 1);
                    positionMap.put("jd_address", jdAddress);
                    logger.info("--------------------------------------------------");
                    logger.info("gearMan call params\t" + JSON.toJSONString(positionMap));
                    logger.info("--------------------------------------------------");
                    try {
                        PositionAddressGearman positionAddress = new PositionAddressGearman(positionId, positionMap);
                        Future<String> result_gearMan = PositionAddressGearman.getThreadPool().submit(positionAddress);
                        String positionResult = result_gearMan.get();
                        String message = positionId + "\tposition gearMan return\t" + positionResult;
                        logger.info(message);
                    } catch (Exception e) {
                        logger.info("call gearMan error," + e.getMessage());
                    }
                }
            }
            return positionId;
        });

        result.foreachRDD(s -> {
            long num = s.count();
            logger.info("process num:" + num);
        });

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
