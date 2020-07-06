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
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author root
 */
public class CvTrade extends GearmanParam implements Callable<String> {

    private List<Map<String, String>> workLists = new ArrayList<>();
    public static String WorkName = "corp_tag";
    public static String FieldName = "cv_trade";
    private static GearmanPool gearmanPool = null;
    private static int ThreadNumber = 2;//执行该work的线程池的最大线程数
    private static ThreadPoolExecutor ThreadPool;
    private static final Logger logger = LoggerFactory.getLogger(CvTrade.class);

    public CvTrade(String resumeId, Map<String, Object> resumeMap) throws Exception {
        super(gearmanPool, WorkName, FieldName);
        super.headerMap.put("product_name", "icdc_flush_cv_trade");
        super.reusmeId = resumeId;
        super.putRequest("cv_id", resumeId);
        super.putRequest("work_list", workLists);
        if (gearmanPool == null) {
            throw new Exception("GearManPool not set");
        }
        process(resumeMap);
    }

    public CvTrade(String resumeId, OriginResumeBean originResumeBean) throws Exception {
        super(gearmanPool, WorkName, FieldName);
        super.headerMap.put("product_name", "icdc_flush_cv_trade");
        super.reusmeId = resumeId;
        if (gearmanPool == null) {
            throw new Exception("GearManPool not set");
        }
        super.putRequest("cv_id", resumeId);
        super.putRequest("work_list", workLists);
        processResume(originResumeBean);
    }

    private void processResume(OriginResumeBean originResumeBean) throws Exception {
        Map<String, Work> works = originResumeBean.getWork();
        if (null == works || works.isEmpty()) {
            throw new Exception("work is null");
        }
        for (Map.Entry<String, Work> entry : works.entrySet()) {
            String work_id = String.valueOf(entry.getKey());
            Work work = entry.getValue();
            putWorkList(work_id, work.getPositionName(), work.getCorporationName(), work.getCorporationDesc(), work.getIndustryName());
        }
    }

    @SuppressWarnings("unchecked")
    private void process(Map<String, Object> resumeMap) throws Exception {
        Object workJson = resumeMap.get("work");
        if (MyObject.isNull(workJson)) {
            throw new Exception("work is null");
        }
        try {
            Map<String, HashMap> works = (Map<String, HashMap>) workJson;
            for (Map.Entry<String, HashMap> entry : works.entrySet()) {
                String work_id = String.valueOf(entry.getKey());
                Map<String, Object> work = (Map<String, Object>) entry.getValue();
                putWorkList(work_id,
                        (String) work.get("position_name"),
                        (String) work.get("corporation_name"),
                        (String) work.get("corporation_desc"),
                        (String) work.get("industry_name"));
            }
        } catch (Exception ex) {
            List works = (List) workJson;
            if (works.isEmpty()) {
                logger.info("CvTrade Works is exception resume_id:" + super.reusmeId);
            }
            int work_id = 0;
            for (Object work : works) {
                Map<String, Object> work_map = (Map<String, Object>) work;
                putWorkList(String.valueOf(work_id),
                        (String) work_map.get("position_name"),
                        (String) work_map.get("corporation_name"),
                        (String) work_map.get("corporation_desc"),
                        (String) work_map.get("industry_name"));
                work_id++;
            }

        }
    }

    private void putWorkList(String work_id, String position_name, String corporation_name, String corporation_desc, String industry_name) {
        Map<String, String> workList = new LinkedHashMap<>();
        workList.put("position", (StringUtils.isNoneBlank(position_name) ? position_name : ""));
        workList.put("company_name", (StringUtils.isNoneBlank(corporation_name) ? corporation_name : ""));
        workList.put("work_id", work_id);
        workList.put("desc", (StringUtils.isNoneBlank(corporation_desc) ? corporation_desc : ""));
        workList.put("industry_name", (StringUtils.isNoneBlank(industry_name) ? industry_name : ""));
        workLists.add(workList);
    }

    @Override
    public boolean packSendMsg() {
        logger.info("cv_trade request:" + JSON.toJSONString(msgMap));
        boolean flag = true;
        try {
            sendMsg = packMsg(msgMap);
        } catch (IOException ex) {
            flag = false;
            logger.info("cv_trade msgPack error," + ex.getMessage());
        }
        return flag;
    }

    @Override
    public boolean parseResult() {
        try {
            Map<String, Value> result_map = unPackMsg(gmResult);
            Value response = result_map.get("response");
            logger.info("cv_trade response:" + response);
            Map<String, Value> responseMap = new Converter(response).read(Templates.tMap(Templates.TString, Templates.TValue));
            int status = new Converter(responseMap.get("status")).read(Templates.TInteger);
            if (status != 0) {
                String msg = "[%s]resume_id:%s GearMan return error, status:%d";
                logger.info(String.format(msg, WorkName, reusmeId, status));
                return false;
            }
            Value result = responseMap.get("result");
            if (result == null) {
                resultJson = "empty";
                return true;
            }
            resultJson = JSON.toJSONString(JSON.parse(result.toString()), SerializerFeature.BrowserCompatible);
        } catch (IOException ex) {
            logger.info("cv_trade msgPack解压结果报错," + ex.getMessage());
            return false;
        }
        return true;
    }

    private static void setGearmanPool() throws Exception {
        gearmanPool = new GearmanPool(WorkName);
        gearmanPool.setMaxNumber((int) (ThreadNumber * 1.25));
    }

    public static GearmanPool getGearmanPool() {
        return gearmanPool;
    }

    public static ThreadPoolExecutor getThreadPool() {
        return ThreadPool;
    }

    public static void init(int thread_number, String worker_name, String worker_field_name) throws Exception {
        if (worker_name != null) {
            WorkName = worker_name;
        }
        if (worker_name != null) {
            FieldName = worker_field_name;
        }
        if (thread_number > 0) {
            ThreadNumber = thread_number;
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
        if (null != workLists && workLists.size() > 0) {
            boolean flag = submit();
            if (flag) {
                result = resultJson;
            }
        }
        return result;
    }
}
