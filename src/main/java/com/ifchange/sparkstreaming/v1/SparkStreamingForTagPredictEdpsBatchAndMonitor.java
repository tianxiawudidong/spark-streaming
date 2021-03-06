package com.ifchange.sparkstreaming.v1;

import com.ifchange.sparkstreaming.v1.common.MysqlConfig;
import com.ifchange.sparkstreaming.v1.mysql.Mysql;
import com.ifchange.sparkstreaming.v1.util.JavaMailUtil;
import com.ifchange.sparkstreaming.v1.util.ParamParseUtil;
import kafka.serializer.StringDecoder;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import javax.mail.Address;
import javax.mail.internet.InternetAddress;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * spark streaming tag_predict_for_edps 日志处理
 * 1、流量监控   5分钟 请求总量、请求总时间
 * 2、性能监控   5分钟 f w 平均响应时间
 * 3、稳定性监控 5分钟 f w 失败率
 * 4、报警监控   5分钟 失败50次
 */
public class SparkStreamingForTagPredictEdpsBatchAndMonitor {

    private static final Logger logger = Logger.getLogger(SparkStreamingForTagPredictEdpsBatchAndMonitor.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static Address[] receiveMails = new Address[2];
    private static Mysql mysql;

    static {
        try {
            logger.info("初始化mysql连接...");
            mysql = new Mysql(MysqlConfig.USERNAME, MysqlConfig.PASSWORD, MysqlConfig.DBNAME, MysqlConfig.HOST, MysqlConfig.PORT);
        } catch (Exception e) {
            logger.info("初始化mysql pool报错," + e.getMessage());
        }

        try {
            receiveMails[0] = new InternetAddress("linhx@ifchange.com", "", "UTF-8");
            receiveMails[1] = new InternetAddress("dongjun.xu@ifchange.com", "", "UTF-8");
        } catch (IOException e) {
            logger.info("加载配置文件报错," + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {

        String appName = "spark-streaming-tag_predict_for_edps-monitor";
        SparkConf conf = new SparkConf();
        conf.setMaster(args[0]);
        conf.setAppName(appName);
        conf.set("app.logging.name", "spark-streaming-tag_predict_for_edps-monitor");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.minutes(1));
        jssc.checkpoint("/algorithm/tag_predict_for_edps-checkpoint");
        String groupId = args[1];
        String topic = args[2];
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "hadoop1:9092,hadoop2:9092,hadoop3:9092,hadoop4:9092,hadoop5:9092,hadoop7:9092");
        // 有smallest、largest、anything可选，分别表示给当前最小的offset、当前最大的offset、抛异常。默认largest
        kafkaParams.put("auto.offset.reset", "largest");
        kafkaParams.put("zookeeper.connect", "hadoop2:2181,hadoop3:2181,hadoop4:2181,hadoop5:2181,hadoop7:2181");
        kafkaParams.put("group.id", groupId);
        Set<String> set = new HashSet<>();
        set.add(topic);
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, set);

        HashPartitioner partitioner = new HashPartitioner(2);

        //1、流量监控
        JavaPairDStream<String, Integer> totalCount = lines.mapToPair((Tuple2<String, String> s) -> {
            Tuple2<String, Integer> tuple2;
            String value = s._2;
            if (StringUtils.isNotBlank(value))
                tuple2 = new Tuple2<>("tag_predict_for_edps", 1);
            else
                tuple2 = new Tuple2<>("tag_predict_for_edps", 0);
            return tuple2;
        }).combineByKey(s -> s, (value1, value2) -> value1 + value2, (value1, value2) -> value1 + value2, partitioner, true)
                .reduceByKeyAndWindow((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2, Durations.minutes(5), Durations.minutes(5), 2);


        JavaPairDStream<String, Double> totalTime = lines.mapToPair(s -> {
            String data = s._2();
            Double responseTime = 0.0;
            if (StringUtils.isNotBlank(data)) {
                Map<String, String> map = null;
                try {
                    map = ParamParseUtil.parse(data);
                } catch (Exception e) {
                    logger.info(data + " 转换成map报错," + e.getMessage());
                }
                if (null != map && map.size() > 0) {
                    String r = StringUtils.isNotBlank(map.get("r")) ? map.get("r") : "0";
                    try {
                        responseTime = Double.parseDouble(r);
                    } catch (NumberFormatException e) {
                        logger.info("响应时间转成double报错," + e.getMessage());
                    }
                }
            }
            return new Tuple2<>("tag_predict_for_edps", responseTime);
        }).combineByKey(s -> s, (value1, value2) -> value1 + value2, (value1, value2) -> value1 + value2, partitioner, true)
                .reduceByKeyAndWindow((Function2<Double, Double, Double>) (v1, v2) -> v1 + v2, Durations.minutes(5), Durations.minutes(5), 2);

        JavaPairDStream<String, Tuple2<Double, Integer>> pair1 = totalTime.join(totalCount, 2);

        //2、性能监控
        JavaPairDStream<String, Double> totalTimeByFAndW = lines.mapToPair(s -> {
            Tuple2<String, Double> tuple2;
            Map<String, String> map = null;
            String value = s._2();
            if (StringUtils.isNotBlank(value)) {
                try {
                    map = ParamParseUtil.parse(value);
                } catch (Exception e) {
                    logger.info(value + "解析报错");
                }
            }
            if (null != map && map.size() > 0) {
                String f = StringUtils.isNotBlank(map.get("f")) ? map.get("f") : "f";
                String w = StringUtils.isNotBlank(map.get("w")) ? map.get("w") : "w";
                String key = f + "+" + w;
                String r = StringUtils.isNotBlank(map.get("r")) ? map.get("r") : "0";
                double responseTime = 0;
                try {
                    responseTime = Double.parseDouble(r);
                } catch (NumberFormatException e) {
                    logger.info("响应时间转成double报错，" + e.getMessage());
                }
                tuple2 = new Tuple2<>(key, responseTime);
            } else {
                tuple2 = new Tuple2<>("f+w", 0.0);
            }
            return tuple2;
        }).combineByKey(s -> s, (a, b) -> a + b, (a, b) -> a + b, partitioner, true)
                .reduceByKeyAndWindow((Function2<Double, Double, Double>) (v1, v2) -> v1 + v2, Durations.minutes(5), Durations.minutes(5), 2);

        JavaPairDStream<String, Integer> totalNumByFAndW = lines.mapToPair(s -> {
            Tuple2<String, Integer> tuple2;
            Map<String, String> map = null;
            String value = s._2();
            if (StringUtils.isNotBlank(value)) {
                try {
                    map = ParamParseUtil.parse(value);
                } catch (Exception e) {
                    logger.info(value + "解析报错");
                }
            }
            if (null != map && map.size() > 0) {
                String f = StringUtils.isNotBlank(map.get("f")) ? map.get("f") : "f";
                String w = StringUtils.isNotBlank(map.get("w")) ? map.get("w") : "w";
                String key = f + "+" + w;
                tuple2 = new Tuple2<>(key, 1);
            } else {
                tuple2 = new Tuple2<>("f+w", 0);
            }
            return tuple2;
        }).combineByKey(s -> s, (a, b) -> a + b, (a, b) -> a + b, partitioner, true)
                .reduceByKeyAndWindow((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2, Durations.minutes(5), Durations.minutes(5), 2);


        //3、稳定性监控
        JavaPairDStream<String, Integer> totalFailCountByFAndW = lines.mapToPair(s -> {
            Tuple2<String, Integer> tuple2;
            Map<String, String> map = null;
            String value = s._2();
            if (StringUtils.isNotBlank(value)) {
                try {
                    map = ParamParseUtil.parse(value);
                } catch (Exception e) {
                    logger.info(value + "解析报错");
                }
            }
            if (null != map && map.size() > 0) {
                String f = StringUtils.isNotBlank(map.get("f")) ? map.get("f") : "f";
                String w = StringUtils.isNotBlank(map.get("w")) ? map.get("w") : "w";
                String success = StringUtils.isNotBlank(map.get("s")) ? map.get("s") : "0";
                String key = f + "+" + w;
                if (success.equals("0"))
                    tuple2 = new Tuple2<>(key, 1);
                else
                    tuple2 = new Tuple2<>(key, 0);
            } else {
                tuple2 = new Tuple2<>("f+w", 0);
            }
            return tuple2;
        }).combineByKey(s -> s, (a, b) -> a + b, (a, b) -> a + b, partitioner, true)
                .reduceByKeyAndWindow((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2, Durations.minutes(5), Durations.minutes(5), 2);

        JavaPairDStream<String, Tuple2<Tuple2<Integer, Double>, Integer>> pair2 = totalFailCountByFAndW.join(totalTimeByFAndW, 2).join(totalNumByFAndW, 2);

        //4、报警监控
        JavaPairDStream<String, Integer> pair3 = lines.map(s -> s._2)
                .filter(s -> {
                    boolean flag = false;
                    if (StringUtils.isNotBlank(s)) {
                        flag = true;
                    }
                    return flag;
                }).mapToPair(s -> {
                    Tuple2<String, Integer> tuple2;
                    Map<String, String> map = null;
                    try {
                        map = ParamParseUtil.parse(s);
                    } catch (Exception e) {
                        logger.info(s + "解析报错!");
                    }
                    if (null != map && map.size() > 0) {
                        String w = map.get("f");
                        //r 频响时间
                        String success = StringUtils.isNotBlank(map.get("s")) ? map.get("s") : "0";
                        if (success.equals("0")) {
                            tuple2 = new Tuple2<>(w, 1);
                        } else {
                            tuple2 = new Tuple2<>(w, 0);
                        }
                    } else {
                        tuple2 = new Tuple2<>("w", 0);
                    }
                    return tuple2;
                }).combineByKey(s -> s, (a, b) -> a + b, (a, b) -> a + b, partitioner, true)
                .reduceByKeyAndWindow((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2,
                        Durations.minutes(5), Durations.minutes(5), 2);

        //流量监控
        pair1.foreachRDD(s -> {
            Tuple2<String, Tuple2<Double, Integer>> data = null;
            try {
                data = s.first();
            } catch (Exception e) {
                logger.info("s.first error," + e.getMessage());
            }
            if (null != data) {
                double times = data._2()._1();
                int num = data._2()._2();
                Calendar instance = Calendar.getInstance();
                instance.add(Calendar.MINUTE, -5);
                Date time = instance.getTime();
                String from = sdf.format(time);
                String to = sdf.format(new Date());
                String sql = "insert into `tag_predict_log_batch_result`(`from_time`,`end_time`,`total_num`,`total_use_time`,`type`)" +
                        "values(\"" + from + "\",\"" + to + "\"," + num + "," + times + "," + 1 + ")";
                logger.info("insert sql==" + sql);
                if (null != mysql) {
                    try {
                        mysql.execute(sql);
                    } catch (SQLException e) {
                        logger.info(sql + " insert into mysql error," + e.getMessage());
                    } finally {
                        mysql.free();
                    }
                }
            }
        });

        //性能监控 稳定性监控
        pair2.foreachRDD(rdd -> {
            long count = 0;
            try {
                count = rdd.count();
            } catch (Exception e) {
                logger.info("rdd.count 报错，" + e.getMessage());
            }
            logger.info("count===" + count);
            if (count > 0) {
                int num = (int) count;
                List<Tuple2<String, Tuple2<Tuple2<Integer, Double>, Integer>>> list = rdd.take(num);
                Calendar instance = Calendar.getInstance();
                instance.add(Calendar.MINUTE, -5);
                Date time = instance.getTime();
                String from = sdf.format(time);
                String to = sdf.format(new Date());
                String type = "tag_predict_for_edps";
                //把数据写入mysql
                String sql = "insert into `echeng_log_request_interval_statistics`(`f`,`work_name`,`from_time`,`end_time`,`avg_response_time`,`fail_rate`,`type_name`) values(?,?,?,?,?,?,?)";
                long time1 = System.currentTimeMillis();
                try {
                    mysql.executeBatchInsertForLog(sql, list, type, from, to);
                    long time2 = System.currentTimeMillis();
                    logger.info("---------------------------------------------");
                    logger.info("batch insert " + count + ",use time:" + (time2 - time1));
                    logger.info("---------------------------------------------");
                } catch (SQLException e) {
                    logger.info("batch insert error," + e.getMessage());
                } finally {
                    mysql.free();
                }
            }
        });

        //报警监控
        pair3.foreachRDD(rdd -> {
            long count = 0;
            try {
                count = rdd.count();
            } catch (Exception e) {
                logger.info("rdd count error" + e.getMessage());
            }
            if (count > 0) {
                int num = (int) count;
                logger.info("count===" + count);
                List<Tuple2<String, Integer>> list = rdd.take(num);
                if (null != list && list.size() > 0) {
                    Calendar instance = Calendar.getInstance();
                    instance.add(Calendar.MINUTE, -5);
                    Date time = instance.getTime();
                    String from = sdf.format(time);
                    String to = sdf.format(new Date());
                    for (Tuple2<String, Integer> tuple : list) {
                        String w = tuple._1();
                        int value = tuple._2();
                        logger.info(w + "-->" + value);
                        String reason = from + "到" + to + ",失败次数:" + value;
                        if (value >= 50) {
                            //发邮件
                            String subject = "TagPredictForEdps日志报警";
                            String sb = "<h1>" +
                                    subject +
                                    "</h1>" +
                                    "<table  border=\"1\">" +
                                    "<tr>" +
                                    "<td>" +
                                    "work_name" +
                                    "</td>" +
                                    "<td>" +
                                    w +
                                    "</td>" +
                                    "</tr>" +
                                    "<tr>" +
                                    "<td>" +
                                    "报警原因" +
                                    "</td>" +
                                    "<td>" +
                                    reason +
                                    "</td>" +
                                    "</table>";
                            JavaMailUtil.sendEmailByIfchange("TagPredict日志报警", sb, receiveMails);
                        }
                    }
                }
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }


}
