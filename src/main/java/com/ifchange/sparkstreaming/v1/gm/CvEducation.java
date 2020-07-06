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
import org.msgpack.template.Templates;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author root
 */
public class CvEducation extends GearmanParam implements Callable<String> {

    private Map<String, Object> educationMap = new HashMap<>();
    public static String WorkName = "cv_education_service_online";
    public static String FieldName = "cv_education";
    //    public static String FieldName1 = "cv_degree";
    private static GearmanPool gearmanPool = null;
    private static int ThreadNumber = 2;//执行该work的线程池的最大线程数
    private static ThreadPoolExecutor ThreadPool;
    private static Logger logger = LoggerFactory.getLogger(CvEducation.class);

    public CvEducation(String resumeId, OriginResumeBean originResumeBean) throws Exception {
        super(gearmanPool, WorkName, FieldName);
        super.reusmeId = resumeId;
        if (gearmanPool == null) {
            throw new Exception("GearManPool not set");
        }
        super.putRequest("c", "CVEducation");
        super.putRequest("m", "query");
        super.putRequest("p", educationMap);
        educationMap.put("basic_degree", originResumeBean.getBasic().getDegree());
        headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        processResume(originResumeBean);
    }

    public CvEducation(String resumeId, String schoolName, String major, String degree) throws Exception {
        super(gearmanPool, WorkName, FieldName);
        super.reusmeId = resumeId;
        if (gearmanPool == null) {
            throw new Exception("GearManPool not set");
        }
        super.putRequest("c", "CVEducation");
        super.putRequest("m", "query");
        List<Map<String, Object>> list = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        map.put("school", schoolName);
        map.put("major", major);
        map.put("degree", degree);
        list.add(map);
        super.putRequest("p", JSON.toJSONString(list));

        headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
    }


    private void processResume(OriginResumeBean originResumeBean) throws Exception {
        Map<String, Education> map = originResumeBean.getEducation();
        if (null == map || map.isEmpty()) {
            throw new Exception("education is null");
        }
        for (Map.Entry<String, Education> entry : map.entrySet()) {
            String education_id = String.valueOf(entry.getKey());
            Education education = entry.getValue();
            putEducationList(education_id, education.getSchoolName(), education.getDisciplineName(), String.valueOf(education.getDegree()));
        }
    }

    private void putEducationList(String education_id, String school_name, String discipline_name, String degree) {
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> educationList = new HashMap<>();
        educationList.put("school", school_name == null ? "" : school_name);
        educationList.put("major", discipline_name == null ? "" : discipline_name);
        educationList.put("degree", degree == null ? "" : degree);
        map.put(education_id, JSON.toJSONString(educationList));
        educationMap.put("educations", map);
    }

    @Override
    public boolean packSendMsg() {
        logger.info("cv_education request:" + JSON.toJSONString(msgMap));
        try {
            sendMsg = packMsg(msgMap);
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean parseResult() {
        try {
            Map<String, Value> result_map = unPackMsg(gmResult);
            Value value = result_map.get("response");
            logger.info("cv_education response:" + value);
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
//            Map<String, Object> education = (Map<String, Object>) JSON.parse(result.toString());
//            Map<String, Object> save_map = new HashMap();
//            Map<String, Object> education_map = (Map<String, Object>) education.get("units");
//            for (String key : education_map.keySet()) {
//                Map<String, Object> save_map_tmp = new HashMap();
//                save_map.put(key, save_map_tmp);
//                Map<String, Object> tmp_emap = (Map<String, Object>) education_map.get(key);
//                save_map_tmp.put("school_id", tmp_emap.get("school_id"));
//                save_map_tmp.put("discipline_id", tmp_emap.get("major_id"));
//            }
//            Map<String, Object> degree_map = (Map<String, Object>) education.get("features");
//            if (degree_map != null) {
//                Object degree = degree_map.get("degree");
//                if (degree != null) {
//                    degreeResult = String.valueOf(degree);
//                }
//            }
//            resultJson = JSON.toJSONString(save_map, SerializerFeature.BrowserCompatible);
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
        if (!educationMap.isEmpty()) {
            boolean flag = submit();
            if (flag) {
                result = resultJson;
            }
        }
        return result;
    }
}
