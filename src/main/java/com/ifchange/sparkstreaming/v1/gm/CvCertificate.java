/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ifchange.sparkstreaming.v1.gm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ifchange.sparkstreaming.v1.common.Environment;
import com.ifchange.sparkstreaming.v1.common.MyExecutor;
import com.ifchange.sparkstreaming.v1.entity.Certificate;
import com.ifchange.sparkstreaming.v1.entity.OriginResumeBean;
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
 * 证书识别
 *
 * @author root
 */
public class CvCertificate extends GearmanParam implements Callable<String> {

    private Map<String, Object> paramMap = new HashMap<>();
    public static String WorkName = "cv_certificate_service";
    public static String FieldName = "cv_certificate";
    private static GearmanPool gearmanPool = null;
    private static int ThreadNumber = 2;//执行该work的线程池的最大线程数
    private static ThreadPoolExecutor ThreadPool;
    private static Logger logger = LoggerFactory.getLogger(CvCertificate.class);

    public CvCertificate(String resumeId, OriginResumeBean originResumeBean) throws Exception {
        super(gearmanPool, WorkName, FieldName);
        super.reusmeId = resumeId;
        headerMap.put("product_name", "cv_certificate");
        if (gearmanPool == null) {
            throw new Exception("GearManPool not set");
        }
        super.putRequest("c", "cv_certificate_service_tag");
        super.putRequest("m", "process");
        super.putRequest("p", paramMap);
        processResume(resumeId, originResumeBean);
    }

    private void processResume(String resumeId, OriginResumeBean originResumeBean) throws Exception {
        Map<String, Certificate> certificate = originResumeBean.getCertificate();
        if (null == certificate || certificate.isEmpty()) {
            throw new Exception("certificate is null");
        }
        Map<String, Object> map = new HashMap<>();
        map.put("certificate", JSON.toJSONString(certificate));
        map.put("resume_id", resumeId);
        String environment = Environment.ENVIRONMENT_1;
        if (environment.equals("development")) {
            paramMap.put("id", resumeId);
            paramMap.put("json", map);
        } else {
            paramMap.put("resume_id", resumeId);
            paramMap.put("certificate", map);
        }

    }

    @Override
    public boolean packSendMsg() {
        logger.info("cv_certificate request" + JSON.toJSONString(msgMap));
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
            logger.info("cv_certificate response:" + value);
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
        if (!paramMap.isEmpty()) {
            logger.info("调用证书算法...");
            boolean flag = submit();
            if (flag) {
                result = resultJson;
            }
        } else {
            logger.info("....不调用证书算法...");
        }
        return result;
    }
}
