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
public class CvLanguage extends GearmanParam implements Callable<String> {

    private Map<String, Object> pMap = new HashMap<>();
    private Map<String, Object> paramMap = new HashMap<>();
    private List<Map<String, Object>> langList = new ArrayList<>();
    public static String WorkName = "cv_lang";
    public static String FieldName = "cv_lang";
    private static GearmanPool gearmanPool = null;
    private static int ThreadNumber = 2;//执行该work的线程池的最大线程数
    private static ThreadPoolExecutor ThreadPool;
    private static final Logger logger = LoggerFactory.getLogger(CvLanguage.class);


    public CvLanguage(String resumeId, Map<String, Object> resumeMap) throws Exception {
        super(gearmanPool, WorkName, FieldName);
        super.reusmeId = String.valueOf(resumeId);
        super.putRequest("c", "cv_lang");
        super.putRequest("m", "get_cv_lang");
        super.putRequest("p", pMap);
        if (gearmanPool == null) {
            throw new Exception("GearManPool not set");
        }
        headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        process(String.valueOf(resumeId), resumeMap);
    }

    public CvLanguage(String resumeId, OriginResumeBean originResumeBean) throws Exception {
        super(gearmanPool, WorkName, FieldName);
        super.reusmeId = String.valueOf(resumeId);
        super.putRequest("c", "cv_lang");
        super.putRequest("m", "get_cv_lang");
        super.putRequest("p", pMap);
        if (gearmanPool == null) {
            throw new Exception("GearManPool not set");
        }
        headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        processResume(resumeId, originResumeBean);
    }

    private void processResume(String resumeId, OriginResumeBean originResumeBean) throws Exception {
        Map<String, Language> languageMap = originResumeBean.getLanguage();
        if (null == languageMap || languageMap.isEmpty()) {
            throw new Exception("language is null");
        }
        paramMap.put("id", resumeId);
        Map<String, Object> langMap = new HashMap<>();
        for (Map.Entry<String, Language> entry : languageMap.entrySet()) {
            Map<String, Object> langValueMap = new HashMap<>();
            String lang_id = entry.getKey();
            Language language = entry.getValue();
            String certificate = StringUtils.isNotBlank(language.getCertificate()) ? language.getCertificate() : "";
            String name = StringUtils.isNotBlank(language.getName()) ? language.getName() : "";
            String level = StringUtils.isNotBlank(language.getLevel()) ? language.getLevel() : "";
            langValueMap.put("certificate", certificate);
            langValueMap.put("name", name);
            langValueMap.put("level", level);
            langMap.put(lang_id, langValueMap);
        }
        paramMap.put("language", langMap);
        langList.add(paramMap);
        pMap.put("cvs", langList);
    }

    @SuppressWarnings("unchecked")
    private void process(String resumeId, Map<String, Object> resumeMap) throws Exception {
        Object languageJson = resumeMap.get("language");
        if (MyObject.isNull(languageJson)) {
            throw new Exception("language is null");
        }
        paramMap.put("id", resumeId);
        Map<String, HashMap> languages = (Map<String, HashMap>) languageJson;
        Iterator<Map.Entry<String, HashMap>> iter = languages.entrySet().iterator();
        Map<String, Object> langMap = new HashMap<>();
        while (iter.hasNext()) {
            Map<String, Object> langValueMap = new HashMap<>();
            Map.Entry<String, HashMap> entry = iter.next();
            String lang_id = String.valueOf(entry.getKey());
            Map<String, Object> language = (Map<String, Object>) entry.getValue();
            String certificate = (String) language.get("certificate");
            certificate = certificate == null ? "" : certificate;
            String name = (String) language.get("name");
            name = name == null ? "" : name;
            String level = (String) language.get("level");
            level = level == null ? "" : level;
            langValueMap.put("certificate", certificate);
            langValueMap.put("name", name);
            langValueMap.put("level", level);
            langMap.put(lang_id, langValueMap);
        }
        paramMap.put("language", langMap);
        langList.add(paramMap);
        pMap.put("cvs", langList);
    }


    @Override
    public boolean packSendMsg() {
        logger.info("cv_language request:" + JSON.toJSONString(msgMap));
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
            logger.info("cv_language response:" + value);
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
            boolean rs = submit();
            if (rs) {
                result = resultJson;
            }
        }
        return result;
    }
}
