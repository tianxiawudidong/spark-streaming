package com.ifchange.sparkstreaming.v1;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * spark streaming kafka direct save offset
 */
public class SparkStreamingForIcdcTest {

    private static final Logger logger = Logger.getLogger(SparkStreamingForIcdcTest.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {

        String appName = "spark-streaming-icdc-monitor";
        SparkConf conf = new SparkConf();
        conf.setMaster(args[0]);
        conf.setAppName(appName);
        conf.set("app.logging.name", "spark-streaming-icdc-monitor");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(30));
        jssc.checkpoint("/basic_data/icdc-checks");
        String groupId = args[1];
        String topic = args[2];
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "192.168.8.194:9092,192.168.8.195:9092,192.168.8.196:9092,192.168.8.197:9092");
        // 有smallest、largest、anything可选，分别表示给当前最小的offset、当前最大的offset、抛异常。默认largest
        kafkaParams.put("auto.offset.reset", "largest");
        kafkaParams.put("zookeeper.connect", "192.168.8.194:2181,192.168.8.195:2181,192.168.8.196:2181,192.168.8.197:2181");
        kafkaParams.put("group.id", groupId);
        Set<String> set = new HashSet<>();
        set.add(topic);



        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream
            (jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, set);

        //从redis读取offset

        //    jssc: JavaStreamingContext,
        //      keyClass: Class[K],
        //      valueClass: Class[V],
        //      keyDecoderClass: Class[KD],
        //      valueDecoderClass: Class[VD],
        //      recordClass: Class[R],
        //      kafkaParams: JMap[String, String],
        //      fromOffsets: JMap[TopicAndPartition, JLong],
        //      messageHandler: JFunction[MessageAndMetadata[K, V], R]
        Map<TopicAndPartition,Long> fromOffsets=new HashMap<>();

        KafkaUtils.createDirectStream(jssc,String.class,String.class,StringDecoder.class,StringDecoder.class,
            String.class,kafkaParams,fromOffsets,new MyMessageHandler());



        JavaDStream<String> result = lines.map((Tuple2<String, String> s) -> {
            String value = s._2;
            logger.info(value);
            return value;
        });


        //流量监控
        result.foreachRDD(s -> {
            long count = s.count();
            logger.info(count);
        });

        lines.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            logger.info("--------------------------------------------------------------");
            Calendar instance = Calendar.getInstance();
            instance.add(Calendar.MINUTE, 0);
            Date time = instance.getTime();
            String times = sdf.format(time);
            for (OffsetRange offsetRange : offsetRanges) {
                String topic1 = offsetRange.topic();
                int partition = offsetRange.partition();
                long start = offsetRange.fromOffset();
                long end = offsetRange.untilOffset();
                logger.info(String.format("time:%s,topic:%s,partition:%d,from_offset:%d,util_offset:%d", times,
                    topic1, partition, start, end));
            }
            logger.info("--------------------------------------------------------------");

            //save offset to redis/zk

        });

        jssc.start();
        jssc.awaitTermination();
    }

    static class MyMessageHandler<MessageAndMetadata> implements Function<MessageAndMetadata,String>{
        @Override
        public String call(MessageAndMetadata messageAndMetadata) throws Exception {
            return null;
        }
    }


}
