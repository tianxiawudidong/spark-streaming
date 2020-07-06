/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ifchange.sparkstreaming.v1.gm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.sparkstreaming.v1.common.MyExecutor;
import com.ifchange.sparkstreaming.v1.entity.*;
import com.ifchange.sparkstreaming.v1.util.JsonBeanUtils;
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
public class CvWexp extends GearmanParam implements Callable<String> {

    private Map<String, Object> pMap = new HashMap<>();
    public static String WorkName = "cv_wkep";
    public static String FieldName = "work_experience";
    private static GearmanPool gearmanPool = null;
    private static int ThreadNumber = 2;//执行该work的线程池的最大线程数
    private static ThreadPoolExecutor ThreadPool;
    private static final Logger logger = LoggerFactory.getLogger(CvWexp.class);

    public CvWexp(String resume_id, Map<String, Object> resumeMap) throws Exception {
        super(gearmanPool, WorkName, FieldName);
        if (gearmanPool == null) {
            throw new Exception("GearManPool not set");
        }
        super.reusmeId = String.valueOf(resume_id);
        Object works = resumeMap.get("work");
        works = (works == null ? "" : works);
        Object educations = resumeMap.get("education");
        educations = (educations == null ? "" : educations);
        pMap.put("work", works);
        pMap.put("education", educations);
        super.putRequest("p", JSON.toJSONString(pMap));
    }

    public CvWexp(String resume_id, OriginResumeBean originResumeBean) throws Exception {
        super(gearmanPool, WorkName, FieldName);
        if (gearmanPool == null) {
            throw new Exception("GearManPool not set");
        }
        super.reusmeId = String.valueOf(resume_id);
        Map<String, Work> workMap = originResumeBean.getWork();
        Map<String, Education> educationMap = originResumeBean.getEducation();
        if (workMap == null || workMap.isEmpty())
            throw new Exception("work is null");
//        if (educationMap == null)
//            throw new Exception("education is null");
        JSONObject workJson = new JSONObject();
        JsonBeanUtils jsonBeanUtils = new JsonBeanUtils();
        for (Map.Entry<String, Work> entry : workMap.entrySet()) {
            String workId = entry.getKey();
            Work work = entry.getValue();
            String works = jsonBeanUtils.objectToJson(work);
            Object workObj = JSONObject.parse(works);
            workJson.put(workId, workObj);
        }
        JSONObject eduJson = new JSONObject();
        if (null != educationMap && educationMap.size() > 0) {
            for (Map.Entry<String, Education> entry : educationMap.entrySet()) {
                String educationId = entry.getKey();
                Education education = entry.getValue();
                String edu = jsonBeanUtils.objectToJson(education);
                Object eduObj = JSONObject.parse(edu);
                eduJson.put(educationId, eduObj);
            }
        }
        pMap.put("work", workJson);
        pMap.put("education", eduJson);
        super.putRequest("p", JSON.toJSONString(pMap));
    }

    @Override
    public boolean packSendMsg() {
        logger.info("cv_wexp request:{}", JSON.toJSONString(msgMap));
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
            logger.info("cv_wexp response:{}", value);
            Map<String, Value> responseMap = new Converter(value).read(Templates.tMap(Templates.TString, Templates.TValue));
            int status = new Converter(responseMap.get("err_no")).read(Templates.TInteger);
            String msg = new Converter(responseMap.get("err_msg")).read(Templates.TString);
            if (status != 0) {
                String showMsg = "[%s]resume_id:%s GearMan return error:%s, status:%d";
                logger.info(String.format(showMsg, WorkName, reusmeId, msg, status));
                logger.info(JSON.toJSONString(msgMap));
                return false;
            }
            Value result = responseMap.get("results");
            if (result == null) {
                resultJson = "empty";
                return true;
            }
            resultJson = result.toString();
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
        boolean flag = submit();
        if (flag) {
            result = resultJson;
        }
        return result;
    }

}
