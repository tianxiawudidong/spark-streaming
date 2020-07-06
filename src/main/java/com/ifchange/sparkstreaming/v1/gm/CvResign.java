/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ifchange.sparkstreaming.v1.gm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ifchange.sparkstreaming.v1.common.MyExecutor;
import com.ifchange.sparkstreaming.v1.entity.*;
import com.ifchange.sparkstreaming.v1.util.JsonBeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.msgpack.template.Templates;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 离职率
 *
 * @author root
 */
public class CvResign extends GearmanParam implements Callable<String> {

    private Map<String, Object> resignMap = new HashMap<>();
    public static String WorkName = "resign_prophet";
    public static String FieldName = "cv_resign";
    private static GearmanPool gearmanPool = null;
    private static int ThreadNumber = 2;//执行该work的线程池的最大线程数
    private static ThreadPoolExecutor ThreadPool;
    private static final Logger logger = LoggerFactory.getLogger(CvResign.class);

    public CvResign(String resumeId, OriginResumeBean originResumeBean, Map<String, Object> behavior, String history) throws Exception {
        super(gearmanPool, WorkName, FieldName);
        super.reusmeId = resumeId;
        if (gearmanPool == null) {
            throw new Exception("GearManPool not set");
        }
        super.putRequest("c", "resign_prediction");
        super.putRequest("m", "resign_computing");
        super.putRequest("p", resignMap);
        headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        processResume(resumeId, originResumeBean, behavior, history);
    }

    public CvResign(String resumeId, String compress, Map<String, Object> behavior, String history) throws Exception {
        super(gearmanPool, WorkName, FieldName);
        super.reusmeId = resumeId;
        if (gearmanPool == null) {
            throw new Exception("GearManPool not set");
        }
        super.putRequest("c", "resign_prediction");
        super.putRequest("m", "resign_computing");
        super.putRequest("p", resignMap);
        headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        process(resumeId, compress, behavior, history);
    }


    @SuppressWarnings("unchecked")
    private void process(String resumeId, String compress, Map<String, Object> behavior, String history) {
        Map<String, Object> map = new HashMap<>();
        map.put("cv_id", resumeId);
        map.put("last_intention", 0);
        map.put("cv_content", compress);
        map.put("behavior", behavior);
        map.put("history", history);
        resignMap.put(resumeId, map);
    }

    private void processResume(String resumeId, OriginResumeBean originResumeBean, Map<String, Object> behavior, String history) {
        Map<String, Object> map = new HashMap<>();
        map.put("cv_id", resumeId);
        map.put("last_intention", 0);
        JsonBeanUtils jsonBeanUtils = new JsonBeanUtils();
        map.put("cv_content", jsonBeanUtils.objectToJson(originResumeBean));
        map.put("behavior", behavior);
        map.put("history", StringUtils.isNoneBlank(history) ? history : "");
        resignMap.put(resumeId, map);
    }

    @Override
    public boolean packSendMsg() {
        logger.info("cv_resign request:" + JSON.toJSONString(msgMap));
        try {
            sendMsg = packMsg(msgMap);
        } catch (IOException ex) {
            return false;
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean parseResult() {
        try {
            Map<String, Value> result_map = unPackMsg(gmResult);
            Value response = result_map.get("response");
            logger.info("cv_resign response:" + response);
            Map<String, Value> responseMap = new Converter(response).read(Templates.tMap(Templates.TString, Templates.TValue));
            int status = Integer.parseInt(new Converter(responseMap.get("err_no")).read(Templates.TString));
            String msg = new Converter(responseMap.get("err_msg")).read(Templates.TString);
            if (status != 0) {
                String showMsg = "***********[%s]resume_id:%s Gearman retrun error:%s, status:%d";
                System.out.println(String.format(showMsg, WorkName, reusmeId, msg, status));
                return false;
            }
            Value result = responseMap.get("results");
            if (result == null) {
                resultJson = "empty";
                return true;
            }
            resultJson = JSON.toJSONString(JSON.parse(result.toString()), SerializerFeature.BrowserCompatible);
        } catch (IOException ex) {
            logger.info(ex.getMessage());
            return false;
        }
        return true;
    }

    private static void setGearmanPool() throws Exception {
        gearmanPool = new GearmanPool(WorkName);
        gearmanPool.setMaxNumber(ThreadNumber * 2);
    }

    public static GearmanPool getGearmanPool() {
        return gearmanPool;
    }

    public static ThreadPoolExecutor getThreadPool() {
        return ThreadPool;
    }

    public static void init(int thead_number, String worker_name, String worker_field_name) throws Exception {
        if (worker_name != null) {
            WorkName = worker_name;
        }
        if (worker_name != null) {
            FieldName = worker_field_name;
        }
        if (thead_number > 0) {
            ThreadNumber = thead_number;
            ThreadPool = (ThreadPoolExecutor) MyExecutor.newFixedThreadPool(ThreadNumber);
            setGearmanPool();
        } else {
            init();
        }
    }

    public static void init() throws Exception {
        ThreadPool = (ThreadPoolExecutor) MyExecutor.newFixedThreadPool(ThreadNumber);
        setGearmanPool();
    }

    @Override
    public String call() {
        String result = "";
        if (!resignMap.isEmpty()) {
            boolean flag = submit();
            if (flag) {
                result = resultJson;
            }
        }
        return result;
    }
}
