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
public class CvTag extends GearmanParam implements Callable<String> {

    private Map<String, Object> pMap = new HashMap<>();
    private Map<String, Object> workMap = new HashMap<>();
    //public static final String WorkName = "cv_tag_online";
    public static String WorkName = "tag_predict";
    public static String FieldName = "cv_tag";
    private static GearmanPool gearmanPool = null;
    private static int ThreadNumber = 2;//执行该work的线程池的最大线程数
    private static ThreadPoolExecutor ThreadPool;
    private static final Logger logger = LoggerFactory.getLogger(CvTag.class);

    public CvTag(String resumeId, OriginResumeBean originResumeBean) throws Exception {
        super(gearmanPool, WorkName, FieldName);
        super.reusmeId = String.valueOf(resumeId);
        if (gearmanPool == null) {
            throw new Exception("GearManPool not set");
        }
        super.putRequest("c", "cv_tag");
        super.putRequest("m", "get_cv_tags");
        super.putRequest("p", pMap);
        headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        pMap.put("cv_id", String.valueOf(resumeId));
        pMap.put("work_map", workMap);
        processResume(originResumeBean);
    }

    @SuppressWarnings("unchecked")
    private void processResume(OriginResumeBean originResumeBean) throws Exception {
        Map<String, Work> workMap = originResumeBean.getWork();
        if (null == workMap || workMap.isEmpty()) {
            throw new Exception("work is null");
        }
        for (Map.Entry<String, Work> entry : workMap.entrySet()) {
            String work_id = entry.getKey();
            Work work = entry.getValue();
            putWorkList(work_id, work.getPositionName(), work.getResponsibilities(), work.getCorporationName(), work.getIndustryName());
        }
    }


    private void putWorkList(String work_id, String positionName, String responsibilities, String corporationName, String industryName) {
        Map<String, Object> workList = new HashMap<>();
        workList.put("id", work_id);
        workList.put("type", 0);
        workList.put("title", (StringUtils.isNoneBlank(positionName) ? positionName : ""));
        workList.put("desc", (StringUtils.isNoneBlank(responsibilities) ? responsibilities : ""));
        workList.put("corporation_name", (StringUtils.isNoneBlank(corporationName) ? corporationName : ""));
        workList.put("industry_name", (StringUtils.isNoneBlank(industryName) ? industryName : ""));
        workMap.put(work_id, workList);
    }

    @Override
    public boolean packSendMsg() {
        logger.info("cv_tag request:" + JSON.toJSONString(msgMap));
//        System.out.println(JSON.toJSONString(msgMap));
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
            Value responseMap = result_map.get("response");
            logger.info("cv_tag response:" + responseMap);
            Map<String, Value> resultMap = new Converter(responseMap).read(Templates.tMap(Templates.TString, Templates.TValue));
            int status = new Converter(resultMap.get("err_no")).read(Templates.TInteger);
            String msg = new Converter(resultMap.get("err_msg")).read(Templates.TString);
            if (status != 0) {
                String showMsg = "[%s]resume_id:%s GearMan return error:%s, status:%d";
                logger.info(String.format(showMsg, WorkName, reusmeId, msg, status));
                return false;
            }
            Value result = resultMap.get("results");
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
        if (!workMap.isEmpty()) {
            boolean rs = submit();
            if (rs) {
                result = resultJson;
            }
        }
        return result;
    }
}
