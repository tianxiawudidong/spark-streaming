/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ifchange.sparkstreaming.v1.gm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ifchange.sparkstreaming.v1.common.MyExecutor;
import com.ifchange.sparkstreaming.v1.common.MyObject;
import com.ifchange.sparkstreaming.v1.entity.*;
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
 * @author root
 */
public class CvTitle extends GearmanParam implements Callable<String> {

    private Map<String, Object> pMap = new HashMap<>();
    public static String WorkName = "title_recognize_server_new_format";
    public static String FieldName = "cv_title";
    private static GearmanPool gearmanPool = null;
    private static int ThreadNumber = 2;//执行该work的线程池的最大线程数
    private static ThreadPoolExecutor ThreadPool;
    private static final Logger logger = LoggerFactory.getLogger(CvTitle.class);


    public CvTitle(String resumeId, Map<String, Object> resumeMap) throws Exception {
        super(gearmanPool, WorkName, FieldName);
        super.reusmeId = resumeId;
        if (gearmanPool == null) {
            throw new Exception("GearManPool not set");
        }
        super.putRequest("c", "title_recognition");
        super.putRequest("m", "get_title_recognition");
        super.putRequest("p", pMap);
        headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        process(resumeMap);
    }

    public CvTitle(String resumeId, OriginResumeBean originResumeBean) throws Exception {
        super(gearmanPool, WorkName, FieldName);
        super.reusmeId = resumeId;
        if (gearmanPool == null) {
            throw new Exception("GearManPool not set");
        }
        super.putRequest("c", "title_recognition");
        super.putRequest("m", "get_title_recognition");
        super.putRequest("p", pMap);
        headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        processResume(originResumeBean);
    }

    private void processResume(OriginResumeBean originResumeBean) throws Exception {
        Map<String, Work> workMap = originResumeBean.getWork();
        if (null == workMap || workMap.isEmpty()) {
            throw new Exception("work is null");
        }
        for (Map.Entry<String, Work> entry : workMap.entrySet()) {
            String work_id = String.valueOf(entry.getKey());
            Work work = entry.getValue();
            String position_name = StringUtils.isNotBlank(work.getPositionName()) ? work.getPositionName() : "";
            putWorkList(work_id, position_name);
        }
    }

    @SuppressWarnings("unchecked")
    private void process(Map<String, Object> resumeMap) throws Exception {
        Object workJson = resumeMap.get("work");
        if (MyObject.isNull(workJson)) {
            throw new Exception("work is null");
        }
        Map<String, HashMap> works = (Map<String, HashMap>) workJson;
        for (Map.Entry<String, HashMap> entry : works.entrySet()) {
            String work_id = String.valueOf(entry.getKey());
            Map<String, Object> work = (Map<String, Object>) entry.getValue();
            String position_name = (String) work.get("position_name");
            position_name = position_name == null ? "" : position_name;
            putWorkList(work_id, position_name);
        }
    }

    private void putWorkList(String work_id, String position_name) {
        pMap.put(work_id, position_name);
    }

    @Override
    public boolean packSendMsg() {
        logger.info("cv_title request:" + JSON.toJSONString(msgMap));
        try {
            sendMsg = packMsg(msgMap);
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean parseResult() {
        try {
            Map<String, Value> result_map = unPackMsg(gmResult);
            Value value = result_map.get("response");
            logger.info("cv_title response:" + value);
            Map<String, Value> responseMap = new Converter(value).read(Templates.tMap(Templates.TString, Templates.TValue));
            int status = new Converter(responseMap.get("err_no")).read(Templates.TInteger);
            String msg = new Converter(responseMap.get("err_msg")).read(Templates.TString);
            if (status != 0) {
                String showMsg = "[%s]resume_id:%s GearMan return error:%s, status:%d";
                logger.info(String.format(showMsg, WorkName, reusmeId, msg, status));
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
        if (!pMap.isEmpty()) {
            boolean flag = submit();
            if (flag) {
                result = resultJson;
            }
        }
        return result;
    }
}
