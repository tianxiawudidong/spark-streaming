package com.ifchange.sparkstreaming.v1.data.hub;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.sparkstreaming.v1.entity.OriginResumeBean;
import com.ifchange.sparkstreaming.v1.entity.ResumeMessage;
import com.ifchange.sparkstreaming.v1.mysql.Mysql;
import com.ifchange.sparkstreaming.v1.mysql.MysqlPool;
import com.ifchange.sparkstreaming.v1.util.CallArthService;
import com.ifchange.sparkstreaming.v1.util.JsonTagsUtils;
import com.ifchange.sparkstreaming.v1.util.ResumeUtil;
import com.ifchange.sparkstreaming.v1.util.WorkUtil;
import kafka.serializer.StringDecoder;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.mortbay.util.ajax.JSON;
import org.msgpack.MessagePack;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SparkStreamingForDataHub {

    private static final Logger logger = Logger.getLogger(SparkStreamingForDataHub.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static MysqlPool mysqlPool1;
    private static MysqlPool mysqlPool2;

    static {
        try {
            mysqlPool1 = new MysqlPool("icdc", "rg62fnme2d68t9cmd3d", "192.168.8.130", 3306);
            mysqlPool2 = new MysqlPool("icdc", "rg62fnme2d68t9cmd3d", "192.168.8.132", 3306);
        } catch (Exception e) {
            logger.info("init mysql pool error," + e.getMessage());
        }
    }


    public static void main(String[] args) throws Exception {

        if (args.length < 5) {
            logger.info("args length is not correct,check it");
            System.exit(1);
        }

        String appName = "spark-streaming-icdc-data-hub";
        SparkConf conf = new SparkConf();
        conf.setMaster(args[0]);
        String groupId = args[1];
        String topic = args[2];
        String broker = args[3];
        String zkConnector = args[4];
        conf.setAppName(appName);
        conf.set("app.logging.name", "spark-streaming-icdc-data-hub");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.minutes(1));
        jssc.checkpoint("/data-hub/icdc");

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", broker);
        // 有smallest、largest、anything可选，分别表示给当前最小的offset、当前最大的offset、抛异常。默认largest
        kafkaParams.put("auto.offset.reset", "largest");
        kafkaParams.put("zookeeper.connect", zkConnector);
        kafkaParams.put("group.id", groupId);
        Set<String> set = new HashSet<>();
        set.add(topic);
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, set);

        final MessagePack messagePack = new MessagePack();

        //1、messagePack解压
        lines.map(kafka -> {
            String resumeId = kafka._1;
            logger.info("receive id:" + resumeId);
            String s = kafka._2;
            OriginResumeBean originResumeBean = null;
            try {
                originResumeBean = messagePack.read(s.getBytes(), OriginResumeBean.class);
            } catch (IOException e) {
                logger.info("message pack error," + e.getMessage());
            }
            return originResumeBean;
        }).map(originResumeBean -> {//2、构造需要调算法参数
            int id = originResumeBean.getBasic().getId();
            int db = ResumeUtil.getDB(id);
            Map<String, Object> behavior = new HashMap<>();
            Mysql mysql = db % 2 == 0 ? mysqlPool1.getMysqlConn() : mysqlPool2.getMysqlConn();
            String sql = "select * from `bi_data`.bi_update_deliver_num where resume_id=" + id;
            logger.info(sql);
            List<Map<String, Object>> list = mysql.executeQuery(sql);
            int days7DeliverNum = 0;
            int days7UpdateNum = 0;
            if (null != list & list.size() > 0) {
                Map<String, Object> map = list.get(0);
                days7DeliverNum = (int) map.get("days7_deliver_num");
                days7UpdateNum = (int) map.get("days7_update_num");
            }
            behavior.put("times_deliver", days7DeliverNum);
            behavior.put("times_update", days7UpdateNum);

            //查询cv_resign
            String dbName = "icdc_" + db;
            String sql2 = "select column_get(data,'cv_resign' as char) as cv_resign from `" + dbName + "`.algorithms where id =" + id;
            String cvResign = "";
            List<Map<String, Object>> list2 = mysql.executeQuery(sql2);
            if (null != list2 && list2.size() > 0) {
                cvResign = String.valueOf(list2.get(0).get("cv_resign"));
            }
            String history = "";
            if (StringUtils.isNotBlank(cvResign)) {
                JSONObject resignJson = JSONObject.parseObject(cvResign);
                if (null != resignJson) {
                    history = resignJson.getString("history");
                }
            }
            JsonTagsUtils jsonTagsUtils = new JsonTagsUtils();
            String callAlgorithms = "";
            String resumeId = String.valueOf(id);
            ResumeMessage resumeMessage = null;
            try {
                resumeMessage = CallArthService.callAlgorithms(callAlgorithms, resumeId, originResumeBean, behavior, history, jsonTagsUtils);
            } catch (Exception e) {
                logger.info("call algorithms error," + e.getMessage());
            }
            if (db % 2 == 0) mysqlPool1.free(mysql);
            else mysqlPool2.free(mysql);
            return resumeMessage;
        }).map(resumeMessage -> {
            //计算cv_source
            if (null != resumeMessage) {
                int id = resumeMessage.getOriginResumeBean().getBasic().getId();
                int db = ResumeUtil.getDB(id);
                String dbName = "icdc_" + db;
                Mysql mysql = db % 2 == 0 ? mysqlPool1.getMysqlConn() : mysqlPool2.getMysqlConn();
                String sql = " select * from `" + dbName + "`.resumes_maps where resume_id =" + id + " and is_deleted='N' limit 1000 ";
                logger.info(sql);
                List<Map<String, Object>> list = mysql.executeQuery(sql);
                String cvSource = JSON.toString(list);
                resumeMessage.getArthTagsResumeBean().setCv_source(cvSource);
                if (db % 2 == 0) mysqlPool1.free(mysql);
                else mysqlPool2.free(mysql);
                //cv_current_status
                int cvCurrentStatus = WorkUtil.getCvCurrentStatus(resumeMessage.getOriginResumeBean());
                resumeMessage.getArthTagsResumeBean().setCv_current_status(cvCurrentStatus);

                //对resumes_extras compress进行压缩->转成16进制



            }

            return resumeMessage;
        });


        jssc.start();
        jssc.awaitTermination();
    }


}
