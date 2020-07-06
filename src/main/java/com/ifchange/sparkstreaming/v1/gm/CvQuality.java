/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ifchange.sparkstreaming.v1.gm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ifchange.sparkstreaming.v1.common.MyExecutor;
import com.ifchange.sparkstreaming.v1.common.MyObject;
import com.ifchange.sparkstreaming.v1.entity.*;
import com.ifchange.sparkstreaming.v1.util.WorkUtil;
import org.apache.commons.lang3.StringUtils;
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
public class CvQuality extends GearmanParam implements Callable<String> {

    private Map<String, Object> pMap = new HashMap<>();
    public static String WorkName = "edps";
    public static String FieldName = "cv_quality";
    private static GearmanPool gearmanPool = null;
    private static int ThreadNumber = 2;//执行该work的线程池的最大线程数
    private static ThreadPoolExecutor ThreadPool;
    private static final Logger logger = LoggerFactory.getLogger(CvQuality.class);

    public CvQuality(String resumeId, OriginResumeBean originResumeBean, String cvTitle, String cvEducation, String cvTag, int workExperience) throws Exception {
        super(gearmanPool, WorkName, FieldName);
        super.reusmeId = resumeId;
        super.putRequest("p", pMap);
        if (gearmanPool == null) {
            throw new Exception("GearManPool not set");
        }
//        try {
            processResume(originResumeBean, cvTitle, cvEducation, cvTag, workExperience);
//        } catch (Exception e) {
//            logger.info("processResume error," + e.getMessage());
//        }
    }

    @SuppressWarnings("unchecked")
    private void processResume(OriginResumeBean originResumeBean, String cvTitle, String cvEducation, String cvTag, int workExperience) throws Exception {
        Map<String, Work> workMap = originResumeBean.getWork();
        if (null == workMap || workMap.isEmpty()) {
            throw new Exception("work is null");
        }
        JSONObject cvTagJson = JSONObject.parseObject(cvTag);
        JSONObject cvEducationJson = JSONObject.parseObject(cvEducation);
        JSONObject cvTitleJson = JSONObject.parseObject(cvTitle);
        //遍历work，把work经历中is_deleted==Y的去掉
        //把workMap转成workList---排序使用
        List<Work> workList = new ArrayList<>();
        for (Map.Entry<String, Work> entry : workMap.entrySet()) {
            String workId = entry.getKey();
            Work work = entry.getValue();
            String isDeleted = StringUtils.isNotBlank(work.getIsDeleted()) ? work.getIsDeleted() : "";
            if (isDeleted.equals("Y")) {
                workMap.remove(workId);
                continue;
            }
            workList.add(work);
        }

        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        // 获取公司名字、传入最近两段工作经历的所有职能ID， 顺序不分先后
        if (workMap.size() > 0) {
            WorkUtil.workSort(workList);
            int funcidCount = 0;
            int i = 0;
            for (Work work : workList) {
                String workId = String.valueOf(work.getId());
                String company = work.getCorporationName();
                sb1.append(company);
                logger.info(company);
                if (i < workList.size() - 1) {
                    sb1.append(",");
                }
                if (null != cvTagJson && null != cvTagJson.getJSONObject(workId) && funcidCount < 2) {
                    String cvTagMust = cvTagJson.getJSONObject(workId).getString("must");
                    if (StringUtils.isNotBlank(cvTagMust)) {
                        logger.info(cvTagMust);
                        if (cvTagMust.contains(":")) {
                            cvTagMust = cvTagMust.substring(cvTagMust.indexOf("\"") + 1, cvTagMust.lastIndexOf("\""));
                            logger.info(cvTagMust);
                            String aa = cvTagMust.split(":")[0];
                            sb2.append(aa);
                        }
                    }
                    if (funcidCount < 1) {
                        sb2.append(",");
                    }
                }
                funcidCount++;
                i++;
            }
        }
        String companyName = sb1.toString();
        String funcIds = sb2.toString();
        logger.info(companyName);
        logger.info(funcIds);

        //职级 1普通职员，2经理，4总监，8VP，16实习生
        Map<String, String> cvTitleAlgorithmConfig = new HashMap<>();
        cvTitleAlgorithmConfig.put("普通职员", "1");
        cvTitleAlgorithmConfig.put("经理", "2");
        cvTitleAlgorithmConfig.put("总监", "4");
        cvTitleAlgorithmConfig.put("VP", "8");
        cvTitleAlgorithmConfig.put("实习生", "16");
        int rankTitleId = 1;
        if (null != cvTitleJson && !cvTitleJson.isEmpty()) {
            for (Map.Entry<String, Object> entry : cvTitleJson.entrySet()) {
                Map<String, Object> map = (Map<String, Object>) entry.getValue();
                String phrase = (String) map.get("phrase");
                if (StringUtils.isNotBlank(phrase)) {
                    String s = cvTitleAlgorithmConfig.get(phrase);
                    if (StringUtils.isNotBlank(s)) {
                        rankTitleId = Integer.parseInt(s);
                    }
                    break;
                }
            }
        }
        // 传入最高学历所在学校名
        Basic basic = originResumeBean.getBasic();
        String schoolName = StringUtils.isNotBlank(basic.getSchoolName()) ? basic.getSchoolName() : "";
        Map<String, Education> educationMap = originResumeBean.getEducation();

        // 传入最高学历所在学校类型，1为211,2为985,0为其他
        int schoolId = 0;
        logger.info("schoolName===" + schoolName);
        if (StringUtils.isNotBlank(schoolName)) {
            //{"features":{"degree":"4"},"units":{"5abe108336456":{"major":"机电一体化技术","school_id":101162,"major_explain":"junior_level3_trie_match","school":"阜阳职业技术学院","degree":4,"school_explain":"school_trie_dict_match","major_id":4560301}}}
            if (!MyObject.isNull(educationMap) && null != cvEducationJson && cvEducationJson.size() > 0) {
                for (Map.Entry<String, Education> entry : educationMap.entrySet()) {
                    String eduId = entry.getKey();
                    logger.info("eduId:" + eduId);
                    Education edu = entry.getValue();
                    if (null != edu) {
                        String isDeleted = edu.getIsDeleted();
                        if (StringUtils.isNotBlank(isDeleted) && isDeleted.equals("Y")) {
                            continue;
                        }
                        String school_name = StringUtils.isNotBlank(edu.getSchoolName()) ? edu.getSchoolName() : "";
                        if (school_name.equals(schoolName)) {
                            JSONObject units = cvEducationJson.getJSONObject("units");
                            if (null != units) {
                                JSONObject eduJson = null != units.getJSONObject(eduId) ? units.getJSONObject(eduId) : units.getJSONObject(eduId.toLowerCase());
                                if (null != eduJson) {
                                    String school_id = eduJson.getString("school_id");
                                    if (StringUtils.isNotBlank(school_id)) {
                                        schoolId = Integer.parseInt(school_id);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            logger.info("schoolId:" + schoolId);
            //如果没有取到school id,重新单独去取
//            if (schoolId == 0) {
//                logger.info("没有取到school id,重新单独去取");
//                CvEducation cvEducation1 = new CvEducation(reusmeId, schoolName, "", "1");
//                Future<String> submitEducation = CvEducation.getThreadPool().submit(cvEducation1);
//                String eduAndDegreeResult = submitEducation.get();
//                logger.info("eduAndDegreeResult:" + eduAndDegreeResult);
//                JSONObject jsonObject = JSON.parseObject(eduAndDegreeResult);
//                if (null != jsonObject) {
//                    JSONObject units = jsonObject.getJSONObject("units");
//                    if (null != units) {
//                        schoolId = null != units.getInteger("school_id") ? units.getInteger("school_id") : 0;
//                    }
//                }
//            }
        }

        // 传入工作年限(向下取整，如1.8年算1年)
        double year = workExperience / 12;
        Double work_experience = Math.floor(year);

        // 传入最高学历对应的学历ID， 1为本科，2为硕士，3为博士，4为大专，6为MBA，0为其他
        String degreeStr = String.valueOf(basic.getDegree());
        int degree = 0;
        try {
            degree = StringUtils.isNotBlank(degreeStr) ? Integer.parseInt(degreeStr) : 0;
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        // 算法那边需要学历组合
        List<Integer> degree_algorithm_config = new ArrayList<>();
        degree_algorithm_config.add(1);
        degree_algorithm_config.add(2);
        degree_algorithm_config.add(3);
        degree_algorithm_config.add(4);
        degree_algorithm_config.add(6);
        if (!degree_algorithm_config.contains(degree)) {
            degree = 0;
        }

        pMap.put("handle", "cvcompetence");
        pMap.put("cvid", Integer.parseInt(reusmeId));
        pMap.put("corpnames", companyName);
        pMap.put("funcids", funcIds);
        pMap.put("ranktitleid", rankTitleId);
        pMap.put("schoolname", schoolName);
        pMap.put("schoolid", schoolId);
        pMap.put("workexpyears", work_experience.intValue());
        pMap.put("degreeid", degree);
        pMap.put("m", "get_cv_quality");
    }


    @Override
    public boolean packSendMsg() {
        logger.info("cv_quality request:" + JSON.toJSONString(msgMap));
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
            logger.info("cv_quality response:" + responseMap);
            Map<String, Value> map = new Converter(responseMap).read(Templates.tMap(Templates.TString, Templates.TValue));
            int status = Integer.parseInt(new Converter(map.get("err_no")).read(Templates.TString));
            String msg = new Converter(map.get("err_msg")).read(Templates.TString);
            if (status != 0) {
                String showMsg = "[%s]resume_id:%s GearMan return error:%s, status:%d";
                logger.info(String.format(showMsg, WorkName, reusmeId, msg, status));
                return false;
            }
            Value result = map.get("results");
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

//    public void run() {
//        try {
//            boolean rs = submit();
//            //添加写入队列
//        } finally {
//            putQueuePool(resultJson);
//        }
//
//    }

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
