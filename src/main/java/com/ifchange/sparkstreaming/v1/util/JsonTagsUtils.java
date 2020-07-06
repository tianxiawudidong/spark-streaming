package com.ifchange.sparkstreaming.v1.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.sparkstreaming.v1.entity.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonTagsUtils {

    private static final Logger logger = LoggerFactory.getLogger(JsonTagsUtils.class);

    @SuppressWarnings("unchecked")
    public Map<String, CvTitleArthBean> parseStrToCvTitleArthBean(String str) {
        Map<String, CvTitleArthBean> result = new HashMap<>();
        JSONObject json = JSONObject.parseObject(str);
        if (null != json && json.size() > 0) {
            for (Map.Entry<String, Object> entry : json.entrySet()) {
                CvTitleArthBean cvTitleArthBean = new CvTitleArthBean();
                String key = entry.getKey();
                Map<String, Object> mapDetail = (Map<String, Object>) entry.getValue();
                cvTitleArthBean.setId(Long.parseLong(String.valueOf(mapDetail.get("id"))));
                cvTitleArthBean.setLevel(Integer.parseInt(String.valueOf(mapDetail.get("level"))));
                cvTitleArthBean.setPhrase(String.valueOf(mapDetail.get("phrase")));
                result.put(key, cvTitleArthBean);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public Map<String, CvTagArthBean> parseStrToCvTagArthBean(String str) {
        Map<String, CvTagArthBean> result = new HashMap<>();
        JSONObject map = JSONObject.parseObject(str);
        if (null != map && map.size() > 0) {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                CvTagArthBean cvTagArthBean = new CvTagArthBean();
                String key = entry.getKey();
                Map<String, Object> mapDetail = (Map<String, Object>) entry.getValue();
                cvTagArthBean.setCategory(String.valueOf(mapDetail.get("category")));
                Object should = mapDetail.get("should");
                Object must = mapDetail.get("must");
                List<String> shouldList = new ArrayList<>();
                List<String> mustList = new ArrayList<>();
                if (null != should) {
                    JSONArray shouldArray = JSON.parseArray(String.valueOf(should));
                    if (null != shouldArray && shouldArray.size() > 0) {
                        for (int j = 0; j < shouldArray.size(); j++) {
                            String shouldStr = shouldArray.getString(j);
                            shouldList.add(shouldStr);
                        }
                    }
                }
                if (null != must) {
                    JSONArray mustArray = JSON.parseArray(String.valueOf(must));
                    if (null != mustArray && mustArray.size() > 0) {
                        for (int j = 0; j < mustArray.size(); j++) {
                            String mustStr = mustArray.getString(j);
                            mustList.add(mustStr);
                        }
                    }
                }
                cvTagArthBean.setMust(mustList);
                cvTagArthBean.setShould(shouldList);
                result.put(key, cvTagArthBean);
            }
        }
        return result;
    }

    //{"5abe0e3cd6075":{"must":[],"category":["52:0.78346"],"title":{"major":[],"skill":[]},"version":2.0,"desc":{"major":[],"skill":[]}}}
    @SuppressWarnings("unchecked")
    public Map<String, CvEntityArthBean> parseStrToCvEntityArthBean(String str) {
        Map<String, CvEntityArthBean> result = new HashMap<>();
        JSONObject map = JSONObject.parseObject(str);
        if (null != map && map.size() > 0) {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                CvEntityArthBean cvEntityArthBean = new CvEntityArthBean();
                String key = entry.getKey();
                Map<String, Object> mapDetail = (Map<String, Object>) entry.getValue();
                //must
                Object must = mapDetail.get("must");
                List<String> mustList = new ArrayList<>();
                if (null != must) {
                    JSONArray mustArray = JSON.parseArray(String.valueOf(must));
                    if (null != mustArray && mustArray.size() > 0) {
                        for (int j = 0; j < mustArray.size(); j++) {
                            String mustStr = mustArray.getString(j);
                            mustList.add(mustStr);
                        }
                    }
                }
                cvEntityArthBean.setMust(mustList);
                //category
                Object category = mapDetail.get("category");
                List<String> categoryList = new ArrayList<>();
                if (null != category) {
                    JSONArray categoryArray = JSON.parseArray(String.valueOf(category));
                    if (null != categoryArray && categoryArray.size() > 0) {
                        for (int j = 0; j < categoryArray.size(); j++) {
                            String categoryStr = categoryArray.getString(j);
                            categoryList.add(categoryStr);
                        }
                    }
                }
                cvEntityArthBean.setCategory(categoryList);

                //title
                CvEntityArthCommon titleCommon = new CvEntityArthCommon();
                Object title = mapDetail.get("title");
                if (null != title) {
                    List<String> majorList = new ArrayList<>();
                    List<String> skillList = new ArrayList<>();
                    Map<String, Object> titleMap = (Map<String, Object>) title;
                    if (!titleMap.isEmpty()) {
                        Object major = titleMap.get("major");
                        Object skill = titleMap.get("skill");
                        if (null != major) {
                            JSONArray majorArray = JSON.parseArray(String.valueOf(major));
                            if (null != majorArray && majorArray.size() > 0) {
                                for (int j = 0; j < majorArray.size(); j++) {
                                    String majorStr = majorArray.getString(j);
                                    majorList.add(majorStr);
                                }
                            }
                        }
                        if (null != skill) {
                            JSONArray skillArray = JSON.parseArray(String.valueOf(skill));
                            if (null != skillArray && skillArray.size() > 0) {
                                for (int j = 0; j < skillArray.size(); j++) {
                                    String skillStr = skillArray.getString(j);
                                    skillList.add(skillStr);
                                }
                            }
                        }
                        titleCommon.setMajor(majorList);
                        titleCommon.setSkill(skillList);
                    }
                }
                cvEntityArthBean.setTitle(titleCommon);

                //desc
                CvEntityArthCommon descCommon = new CvEntityArthCommon();
                Object desc = mapDetail.get("desc");
                if (null != desc) {
                    List<String> majorList = new ArrayList<>();
                    List<String> skillList = new ArrayList<>();
                    Map<String, Object> titleMap = (Map<String, Object>) title;
                    if (null != titleMap && !titleMap.isEmpty()) {
                        Object major = titleMap.get("major");
                        Object skill = titleMap.get("skill");
                        if (null != major) {
                            JSONArray majorArray = JSON.parseArray(String.valueOf(major));
                            if (null != majorArray && majorArray.size() > 0) {
                                for (int j = 0; j < majorArray.size(); j++) {
                                    String majorStr = majorArray.getString(j);
                                    majorList.add(majorStr);
                                }
                            }
                        }
                        if (null != skill) {
                            JSONArray skillArray = JSON.parseArray(String.valueOf(skill));
                            if (null != skillArray && skillArray.size() > 0) {
                                for (int j = 0; j < skillArray.size(); j++) {
                                    String skillStr = skillArray.getString(j);
                                    skillList.add(skillStr);
                                }
                            }
                        }
                        descCommon.setMajor(majorList);
                        descCommon.setSkill(skillList);
                    }
                }
                cvEntityArthBean.setDesc(descCommon);

                //version
                double version = Double.parseDouble(mapDetail.get("version").toString());
                cvEntityArthBean.setVersion(version);
                result.put(key, cvEntityArthBean);
            }
        }
        return result;
    }

    //{"features":{"degree":"4"},"units":{"5ABE108336456":{"major":"机电一体化技术","school_id":101162,"major_explain":"junior_level3_trie_match","school":"阜阳职业技术学院","degree":4,"school_explain":"school_trie_dict_match","major_id":4560301}}}
    @SuppressWarnings("unchecked")
    public CvEducationArthBean parseStrToCvEducationArthBean(String str) {
        CvEducationArthBean result = new CvEducationArthBean();
        CvEducationFeatures cvEducationFeatures = new CvEducationFeatures();
        Map<String, CvEducationUnit> unitsMap = new HashMap<>();
        JSONObject map = JSONObject.parseObject(str);
        if (null != map && map.size() > 0) {
            JSONObject features = map.getJSONObject("features");
            if (null != features) {
                String degree = StringUtils.isNotBlank(features.getString("degree")) ? features.getString("degree") : "";
                cvEducationFeatures.setDegree(degree);
            }
            JSONObject units = map.getJSONObject("units");
            if (null != units && units.size() > 0) {
                for (Map.Entry<String, Object> entry : units.entrySet()) {
                    CvEducationUnit cvEducationUnit = new CvEducationUnit();
                    String key = entry.getKey();
                    Map<String, Object> mapDetail = (Map<String, Object>) entry.getValue();
                    //major
                    Object major = mapDetail.get("major");
                    if (null != major) {
                        String majorStr = String.valueOf(major);
                        cvEducationUnit.setMajor(majorStr);
                    }
                    //school_id
                    Object school_id = mapDetail.get("school_id");
                    if (null != school_id) {
                        long schoolId = Long.parseLong(String.valueOf(school_id));
                        cvEducationUnit.setSchoolId(schoolId);
                    }
                    //major_explain
                    Object majorExplain = mapDetail.get("major_explain");
                    if (null != majorExplain) {
                        String majorExplainStr = String.valueOf(majorExplain);
                        cvEducationUnit.setMajorExplain(majorExplainStr);
                    }
                    //school
                    Object school = mapDetail.get("school");
                    if (null != school) {
                        String schoolStr = String.valueOf(school);
                        cvEducationUnit.setSchool(schoolStr);
                    }
                    Object degreeObj = mapDetail.get("degree");
                    if (null != degreeObj) {
                        int degree = Integer.parseInt(String.valueOf(degreeObj));
                        cvEducationUnit.setDegree(degree);
                    }
                    //major_id
                    Object major_id = mapDetail.get("major_id");
                    if (null != major_id) {
                        long majorId = Long.parseLong(String.valueOf(major_id));
                        cvEducationUnit.setMajorId(majorId);
                    }
                    //school_explain
                    Object school_explain = mapDetail.get("school_explain");
                    if (null != school_explain) {
                        String schoolExplain = String.valueOf(school_explain);
                        cvEducationUnit.setSchoolExplain(schoolExplain);
                    }
                    unitsMap.put(key, cvEducationUnit);
                }
            }
        }
        result.setFeatures(cvEducationFeatures);
        result.setUnits(unitsMap);
        return result;
    }

    public CvFeatureArthBean parseStrToCvFeatureArthBean(String str) {
        CvFeatureArthBean cvFeatureArthBean = new CvFeatureArthBean();
        JSONObject json = JSONObject.parseObject(str);
        if (null != json && json.size() > 0) {
            List<CvFeatureArthWork> workList = new ArrayList<>();
            JSONArray workArray = json.getJSONArray("work");
            if (null != workArray && workArray.size() > 0) {
                for (int j = 0; j < workArray.size(); j++) {
                    CvFeatureArthWork cvFeatureArthWork = new CvFeatureArthWork();
                    JSONObject jsonObject = workArray.getJSONObject(j);
                    cvFeatureArthWork.setStartTime(jsonObject.getString("start_time"));
                    cvFeatureArthWork.setEndTime(jsonObject.getString("end_time"));
                    String workId = jsonObject.getString("work_id");
                    cvFeatureArthWork.setWorkId(workId);

                    Map<String, Object> desc = jsonObject.getJSONObject("desc");
                    Map<String, String> descMap = new HashMap<>();
                    for (Map.Entry<String, Object> entry : desc.entrySet()) {
                        descMap.put(entry.getKey(), String.valueOf(entry.getValue()));
                    }
                    cvFeatureArthWork.setDesc(descMap);
                    JSONArray descEntityArray = jsonObject.getJSONArray("desc_entity");
                    List<String> descEntityList = new ArrayList<>();
                    if (null != descEntityArray && descEntityArray.size() > 0) {
                        for (int m = 0; m < descEntityArray.size(); m++) {
                            descEntityList.add(descEntityArray.getString(m));
                        }
                    }
                    cvFeatureArthWork.setDescEntity(descEntityList);

                    JSONArray vectorArray = jsonObject.getJSONArray("vector");
                    List<String> vectorList = new ArrayList<>();
                    if (null != vectorArray && vectorArray.size() > 0) {
                        for (int m = 0; m < vectorArray.size(); m++) {
                            vectorList.add(vectorArray.getString(m));
                        }
                    }
                    cvFeatureArthWork.setVector(vectorList);

                    Map<String, Object> title = jsonObject.getJSONObject("title");
                    Map<String, String> titleMap = new HashMap<>();
                    for (Map.Entry<String, Object> entry : title.entrySet()) {
                        titleMap.put(entry.getKey(), String.valueOf(entry.getValue()));
                    }
                    cvFeatureArthWork.setTitle(titleMap);

                    JSONArray titleEntityArray = jsonObject.getJSONArray("title_entity");
                    List<String> titleEntityList = new ArrayList<>();
                    if (null != titleEntityArray && titleEntityArray.size() > 0) {
                        for (int m = 0; m < titleEntityArray.size(); m++) {
                            titleEntityList.add(titleEntityArray.getString(m));
                        }
                    }
                    cvFeatureArthWork.setTitleEntity(titleEntityList);
                    workList.add(cvFeatureArthWork);
                }
            }
            cvFeatureArthBean.setWork(workList);
        }
        return cvFeatureArthBean;
    }

    public CvQualityArthBean parseStrToCvQualityArthBean(String str) {
        CvQualityArthBean cvQualityArthBean = new CvQualityArthBean();
        JSONObject json = JSONObject.parseObject(str);
        if (null != json && json.size() > 0) {
            cvQualityArthBean.setScore(json.getDouble("score"));
            cvQualityArthBean.setScoreDetail(json.getString("score_detail"));
            cvQualityArthBean.setTotalCount(json.getInteger("totalcount"));
        }
        return cvQualityArthBean;
    }

    public List<CvTradeArthBean> parseStrToCvTradeArthBean(String str) {
        List<CvTradeArthBean> list = new ArrayList<>();
        JSONArray array = JSONArray.parseArray(str);
        if (null != array && array.size() > 0) {
            for (int i = 0; i < array.size(); i++) {
                CvTradeArthBean cvTradeArthBean = new CvTradeArthBean();
                JSONObject json = array.getJSONObject(i);
                cvTradeArthBean.setCompanyId(json.getLong("company_id"));

                JSONObject companyInfoJson = json.getJSONObject("company_info");
                CvTradeArthCompanyInfo companyInfo = new CvTradeArthCompanyInfo();
                companyInfo.setInternalId(companyInfoJson.getLong("internal_id"));
                companyInfo.setInternalName(companyInfoJson.getString("internal_name"));
                List<String> companyTypeList = new ArrayList<>();
                JSONArray companyTypeArray = companyInfoJson.getJSONArray("company_type");
                if (null != companyTypeArray && companyTypeArray.size() > 0) {
                    for (int a = 0; a < companyTypeArray.size(); a++) {
                        companyTypeList.add(companyTypeArray.getString(a));
                    }
                }
                companyInfo.setCompanyType(companyTypeList);

                List<String> keywordList = new ArrayList<>();
                JSONArray keywordArray = companyInfoJson.getJSONArray("keyword");
                if (null != keywordArray && keywordArray.size() > 0) {
                    for (int a = 0; a < keywordArray.size(); a++) {
                        keywordList.add(keywordArray.getString(a));
                    }
                }
                companyInfo.setKeyword(keywordList);

                List<String> regionList = new ArrayList<>();
                JSONArray regionArray = companyInfoJson.getJSONArray("region");
                if (null != regionArray && regionArray.size() > 0) {
                    for (int a = 0; a < regionArray.size(); a++) {
                        regionList.add(regionArray.getString(a));
                    }
                }
                companyInfo.setRegion(regionList);
                cvTradeArthBean.setCompanyInfo(companyInfo);
                cvTradeArthBean.setCompanyName(json.getString("company_name"));
                cvTradeArthBean.setCorpCluster(json.getString("corp_cluster"));

                List<String> firstTradeList = new ArrayList<>();
                JSONArray firstTradeListArray = json.getJSONArray("first_trade_list");
                if (null != firstTradeListArray && firstTradeListArray.size() > 0) {
                    for (int a = 0; a < firstTradeListArray.size(); a++) {
                        firstTradeList.add(firstTradeListArray.getString(a));
                    }
                }
                cvTradeArthBean.setFirstTradeList(firstTradeList);
                cvTradeArthBean.setNormCorpId(json.getLong("norm_corp_id"));
                cvTradeArthBean.setNormParentCorpId(json.getLong("norm_parent_corp_id"));

                List<String> secondTradeList = new ArrayList<>();
                JSONArray secondTradeListArray = json.getJSONArray("second_trade_list");
                if (null != secondTradeListArray && secondTradeListArray.size() > 0) {
                    for (int a = 0; a < secondTradeListArray.size(); a++) {
                        secondTradeList.add(secondTradeListArray.getString(a));
                    }
                }
                cvTradeArthBean.setSecondTradeList(secondTradeList);
                String workId = json.getString("work_id");
                cvTradeArthBean.setWorkId(workId);
                list.add(cvTradeArthBean);
            }
        }
        return list;
    }

    //cv_language
    public CvLanguageArthBean parseStrToCvLanguageArthBean(String str) {
        CvLanguageArthBean CvLanguageArthBean = new CvLanguageArthBean();
        JSONObject json = JSONObject.parseObject(str);
        JSONArray cvsArray = json.getJSONArray("cvs");
        List<CvLanguageArthCVS> cvsList = new ArrayList<>();
        if (null != cvsArray && cvsArray.size() > 0) {
            for (int i = 0; i < cvsArray.size(); i++) {
                CvLanguageArthCVS cvLanguageArthCVS = new CvLanguageArthCVS();
                JSONObject jsonDetail = cvsArray.getJSONObject(i);
                cvLanguageArthCVS.setId(jsonDetail.getLong("id"));
                List<CvLanguageArthLanguage> list = new ArrayList<>();
                JSONArray language = jsonDetail.getJSONArray("language");
                if (null != language && language.size() > 0) {
                    for (int j = 0; j < language.size(); j++) {
                        CvLanguageArthLanguage cvLanguageArthLanguage = new CvLanguageArthLanguage();
                        JSONObject languageJSONObject = language.getJSONObject(j);
                        cvLanguageArthLanguage.setDetail(languageJSONObject.getString("detail"));
                        cvLanguageArthLanguage.setLevel(languageJSONObject.getString("level"));
                        cvLanguageArthLanguage.setName(languageJSONObject.getString("name"));
                        list.add(cvLanguageArthLanguage);
                    }
                }
                cvLanguageArthCVS.setLanguage(list);
                cvsList.add(cvLanguageArthCVS);
            }
        }
        CvLanguageArthBean.setCvs(cvsList);
        return CvLanguageArthBean;
    }

    //cv_resign
    //{"4875691":{"resign_intention":"-0.28","history":"","resign_features":{}}}
//    :{"14849381":{"resign_features":{1:{"name":"年龄段","weight":0.021660649819494584},
// 4:{"name":"学校类别","weight":0.02707581227436823},
// 5:{"name":"更换工作城市","weight":0.0018050541516245488},6:{"name":"工作经验","weight":0.20938628158844766},7:{"name":"薪水","weight":0.09386281588447654},8:{"name":"当前工作时间","weight":0.2148014440433213},9:{"name":"最大工作时间","weight":0.052346570397111915},10:{"name":"最小工作时间","weight":0.0812274368231047},11:{"name":"平均工作时间","weight":0.08664259927797834},12:{"name":"当前工作时间与平均工作时间比值","weight":0.11010830324909747},13:{"name":"薪水与平均工作时间比值","weight":0.05054151624548736},14:{"name":"薪水与行业基准比值","weight":0.010830324909747292},15:{"name":"薪水与职能基准比值","weight":0.01263537906137184},16:{"name":"薪水与地域基准比值","weight":0.01263537906137184},17:{"name":"职能的平均供需比,人才紧缺，供不应求","weight":0.0},24:{"name":"当前在职时间与(职能下的)职级平均晋升时间比值,晋升速度","weight":0.01444043321299639}},"resign_intention":"0.508343297286","history":"[['2018-04-11-20-14-32', '2018-04-11-20-14-32', 0, 0, '0.508343297286']]"}}}
    @SuppressWarnings("unchecked")
    public Map<String, CvResignArthBean> parseStrToCvResignArthBean(String str) {
        Map<String, CvResignArthBean> cvResignMap = new HashMap<>();
        CvResignArthBean cvResignArthBean = new CvResignArthBean();
        JSONObject json = JSONObject.parseObject(str);
        for (Map.Entry<String, Object> map : json.entrySet()) {
            String resumeId = map.getKey();
            JSONObject valueJson = (JSONObject) map.getValue();
            cvResignArthBean.setResignIntention(valueJson.getString("resign_intention"));
            cvResignArthBean.setHistory(valueJson.getString("history"));
            Map<String, CvResignArthResignFeatures> resignFeatures = new HashMap<>();
            JSONObject resignFeaturesJson = valueJson.getJSONObject("resign_features");
            if (null != resignFeaturesJson && resignFeaturesJson.size() > 0) {
                for (Map.Entry<String, Object> entry : resignFeaturesJson.entrySet()) {
                    String key = entry.getKey();
                    Map<String, Object> value = (Map<String, Object>) entry.getValue();
                    CvResignArthResignFeatures cvResignArthResignFeatures = new CvResignArthResignFeatures();
                    cvResignArthResignFeatures.setName(String.valueOf(value.get("name")));
                    cvResignArthResignFeatures.setWeight((BigDecimal) value.get("weight"));
                    resignFeatures.put(key, cvResignArthResignFeatures);
                }
            }
            cvResignArthBean.setResignFeatures(resignFeatures);
            cvResignMap.put(resumeId, cvResignArthBean);
        }
        return cvResignMap;
    }

    //cv_certificate
    //开发环境:{"5000003":[3002686,4001298]}
    //测试环境:{"4875691":{"4001095":1.0,"3002451":1.0}}
    public Map<String, String> parseStrToCvCertificateArthBean(String str) {
        JSONObject jsonObject = JSONObject.parseObject(str);
        Map<String, String> certificateIds = new HashMap<>();
        if (null != jsonObject && jsonObject.size() > 0) {
            for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                String key = entry.getKey();
                try {
                    String value = String.valueOf(entry.getValue());
                    certificateIds.put(key, value);
                } catch (Exception e) {
                    logger.info("str parse error" + e.getMessage());
                }
            }
        }
        return certificateIds;
    }

}
