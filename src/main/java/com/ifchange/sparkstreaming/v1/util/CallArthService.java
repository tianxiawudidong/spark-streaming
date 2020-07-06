package com.ifchange.sparkstreaming.v1.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.sparkstreaming.v1.entity.*;
import com.ifchange.sparkstreaming.v1.gm.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * 调用算法封装
 */
public class CallArthService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CallArthService.class);
    private static final Logger CV_TRADE_LOG = LoggerFactory.getLogger("CV_TRADE_ALGORITHMS_LOGGER");
    private static final Logger CV_TAG_LOG = LoggerFactory.getLogger("CV_TAG_ALGORITHMS_LOGGER");
    private static final Logger CV_TITLE_LOG = LoggerFactory.getLogger("CV_TITLE_ALGORITHMS_LOGGER");
    private static final Logger CV_ENTITY_LOG = LoggerFactory.getLogger("CV_ENTITY_ALGORITHMS_LOGGER");
    private static final Logger CV_EDUCATION_LOG = LoggerFactory.getLogger("CV_EDUCATION_ALGORITHMS_LOGGER");
    private static final Logger CV_FEATURE_LOG = LoggerFactory.getLogger("CV_FEATURE_ALGORITHMS_LOGGER");
    private static final Logger CV_QUALITY_LOG = LoggerFactory.getLogger("CV_QUALITY_ALGORITHMS_LOGGER");
    private static final Logger CV_LANGUAGE_LOG = LoggerFactory.getLogger("CV_LANGUAGE_ALGORITHMS_LOGGER");
    private static final Logger CV_RESIGN_LOG = LoggerFactory.getLogger("CV_RESIGN_ALGORITHMS_LOGGER");
    private static final Logger CV_WORKYEAR_LOG = LoggerFactory.getLogger("CV_WORKYEAR_ALGORITHMS_LOGGER");
    private static final Logger CV_CERTIFICATE_LOG = LoggerFactory.getLogger("CV_CERTIFICATE_ALGORITHMS_LOGGER");
    private static final Logger ARTH_LOG = LoggerFactory.getLogger("ALGORITHMS_TIME_LOGGER");


    @SuppressWarnings("unchecked")
    public static ResumeMessage callAlgorithms(String callAlgorithms, String resumeId, OriginResumeBean originResumeBean,
                                               Map<String, Object> behavior, String history, JsonTagsUtils jsonTagsUtils) {
        long arthStart = System.currentTimeMillis();
        ResumeMessage resumeMessage = new ResumeMessage();
        ArthTagsResumeBean arthTagsResumeBean = new ArthTagsResumeBean();
        //调用cv_quality需要
        String titleResult = "";
        //调用cv_quality需要
        String tagResult = "";
        //调用cv_quality需要
        String eduResult = "";
        //调用cv_quality需要
        String wexpResult = "0";
        Map<String, Work> workMap = originResumeBean.getWork();
        Basic basic = originResumeBean.getBasic();
        //cv_trade公司和行业识别
        if (callAlgorithms.contains("cv_trade")) {
            try {
                CV_TRADE_LOG.info("...call cv_trade...");
                long start = System.currentTimeMillis();
                CvTrade cvTrade = new CvTrade(resumeId, originResumeBean);
                Future<String> submit = CvTrade.getThreadPool().submit(cvTrade);
                String tradeResult = submit.get();
                long end = System.currentTimeMillis();
                CV_TRADE_LOG.info("cv_trade result:[" + tradeResult + "]");
                CV_TRADE_LOG.info("cv_trade use time:" + (end - start));
                LOGGER.info("cv_trade use time:" + (end - start));
                List<CvTradeArthBean> cvTradeArthBeans = new ArrayList<>();
                if (StringUtils.isNotBlank(tradeResult)) {
                    try {
                        cvTradeArthBeans = jsonTagsUtils.parseStrToCvTradeArthBean(tradeResult);
                    } catch (Exception e) {
                        CV_TRADE_LOG.info("cv_trade result parse to bean error," + e.getMessage());
                    }
                }
                arthTagsResumeBean.setCv_trade(cvTradeArthBeans);
                JSONArray array = JSONArray.parseArray(tradeResult);
                if (null != array && array.size() > 0) {
                    for (int i = 0; i < array.size(); i++) {
                        JSONObject json = array.getJSONObject(i);
                        String workId = json.getString("work_id");
                        Work work = workMap.get(workId);
                        if (null != work) {
                            //将识别过后的公司id回写到压缩包里面
                            String companyId = StringUtils.isNotBlank(json.getString("company_id")) ? json.getString("company_id") : "0";
                            work.setCorporationId(Integer.parseInt(companyId));

                            JSONArray firstTradeList = json.getJSONArray("first_trade_list");
                            //以逗号拼接
                            StringBuilder sb = new StringBuilder();
                            if (null != firstTradeList && firstTradeList.size() > 0) {
                                for (int j = 0; j < firstTradeList.size(); j++) {
                                    String trade = firstTradeList.getString(j);
                                    sb.append(trade);
                                    if (j != firstTradeList.size() - 1) {
                                        sb.append(",");
                                    }
                                }
                            }
                            String industryIds = StringUtils.isNotBlank(sb.toString()) ? sb.toString() : "";
                            //将识别的industry_ids  回写到压缩包里面
                            if (StringUtils.isNotBlank(industryIds)) {
                                work.setIndustryIds(Integer.parseInt(industryIds));
                            }
                            workMap.put(workId, work);
                            originResumeBean.setWork(workMap);
                        }
                    }
                }
                //取出最新的工作经历，并将里面的属性写回basic中(除了id、updated_at、is_deleted)
                Work currentWork = WorkUtil.getCurrentWork(originResumeBean);
                basic.setStartTime(currentWork.getStartTime());
                basic.setEndTime(currentWork.getEndTime());
                basic.setSoFar(currentWork.getSoFar());
                basic.setCorporationName(currentWork.getCorporationName());
                basic.setIndustryName(currentWork.getIndustryName());
                basic.setArchitectureName(currentWork.getArchitectureName());
                basic.setPositionName(currentWork.getPositionName());
                basic.setTitleName(currentWork.getTitleName());
                basic.setStationName(currentWork.getStationName());
                basic.setReportingTo(currentWork.getReportingTo());
                basic.setSubordinatesCount(currentWork.getSubordinatesCount());
                basic.setResponsibilities(currentWork.getResponsibilities());
                basic.setManagementExperience(currentWork.getManagementExperience());
                basic.setWorkType(currentWork.getWorkType());
                basic.setBasicSalary(currentWork.getBasicSalary());
                basic.setBonus(currentWork.getBonus());
                basic.setAnnualSalary(currentWork.getAnnualSalary());
                basic.setBasicSalaryFrom(currentWork.getBasicSalaryFrom());
                basic.setBasicSalaryTo(currentWork.getBasicSalaryTo());
                basic.setSalaryMonth(currentWork.getSalaryMonth());
                basic.setAnnualSalaryFrom(currentWork.getAnnualSalaryFrom());
                basic.setAnnualSalaryTo(currentWork.getAnnualSalaryTo());
                basic.setCorporationDesc(currentWork.getCorporationDesc());
                basic.setScale(currentWork.getScale());
                basic.setCity(currentWork.getCity());
                basic.setCorporationType(currentWork.getCorporationType());
                basic.setReason(currentWork.getReason());
                basic.setIsOversea(currentWork.getIs_oversea());
                basic.setAchievement(currentWork.getAchievement());
                basic.setaPB(currentWork.getaPB());
                basic.setCorporationId(currentWork.getCorporationId());
                basic.setIndustryIds(String.valueOf(currentWork.getIndustryIds()));
                basic.setCreatedAt(currentWork.getCreatedAt());
                basic.setArchitectureId(String.valueOf(currentWork.getArchitectureId()));
                basic.setCompanyInfo(currentWork.getCompanyInfo());
                originResumeBean.setBasic(basic);
            } catch (Exception e) {
                CV_TRADE_LOG.info("call cv_trade error," + e.getMessage());
            }
        }
        resumeMessage.setOriginResumeBean(originResumeBean);
        //cv_title职级
        if (callAlgorithms.contains("cv_title")) {
            try {
                CV_TITLE_LOG.info("...call cv_title...");
                long start = System.currentTimeMillis();
                CvTitle cvTitle = new CvTitle(resumeId, originResumeBean);
                Future<String> submitTitle = CvTitle.getThreadPool().submit(cvTitle);
                titleResult = submitTitle.get();
                long end = System.currentTimeMillis();
                CV_TITLE_LOG.info("cv_title result:[" + titleResult + "]");
                CV_TITLE_LOG.info("cv_title use time:" + (end - start));
                LOGGER.info("cv_title use time:" + (end - start));
                Map<String, CvTitleArthBean> stringCvTitleArthBeanMap = new HashMap<>();
                if (StringUtils.isNotBlank(titleResult)) {
                    try {
                        stringCvTitleArthBeanMap = jsonTagsUtils.parseStrToCvTitleArthBean(titleResult);
                    } catch (Exception e) {
                        CV_TITLE_LOG.info("cv_title result parse to bean error," + e.getMessage());
                    }
                }
                arthTagsResumeBean.setCv_title(stringCvTitleArthBeanMap);
            } catch (Exception e) {
                CV_TITLE_LOG.info("call cv_title error," + e.getMessage());
            }
        }
        //cv_tag职能/标签
        if (callAlgorithms.contains("cv_tag")) {
            try {
                CV_TAG_LOG.info("...call cv_tag...");
                long start = System.currentTimeMillis();
                CvTag cvTag = new CvTag(resumeId, originResumeBean);
                Future<String> submitTag = CvTag.getThreadPool().submit(cvTag);
                tagResult = submitTag.get();
                long end = System.currentTimeMillis();
                CV_TAG_LOG.info("cv_tag result:" + tagResult + "");
                CV_TAG_LOG.info("cv_tag use time:" + (end - start));
                LOGGER.info("cv_tag use time:" + (end - start));
                Map<String, CvTagArthBean> maps = new HashMap<>();
                if (StringUtils.isNotBlank(tagResult)) {
                    try {
                        maps = jsonTagsUtils.parseStrToCvTagArthBean(tagResult);
                    } catch (Exception e) {
                        CV_TAG_LOG.info("cv_tag result parse to bean error," + e.getMessage());
                    }
                }
                arthTagsResumeBean.setCv_tag(maps);
            } catch (Exception e) {
                CV_TAG_LOG.info("call cv_tag error," + e.getMessage());
            }
        }
        //cv_entity职能职级
        if (callAlgorithms.contains("cv_entity")) {
            try {
                CV_ENTITY_LOG.info("...call cv_entity...");
                long start = System.currentTimeMillis();
                CvEntity cvEntity = new CvEntity(resumeId, originResumeBean);
                Future<String> submitEntity = CvEntity.getThreadPool().submit(cvEntity);
                String entityResult = submitEntity.get();
                long end = System.currentTimeMillis();
                CV_ENTITY_LOG.info("cv_entity result:[" + entityResult + "]");
                CV_ENTITY_LOG.info("cv_entity use time:" + (end - start));
                LOGGER.info("cv_entity use time:" + (end - start));
                Map<String, CvEntityArthBean> cvEntityArthBean = new HashMap<>();
                if (StringUtils.isNotBlank(entityResult)) {
                    try {
                        cvEntityArthBean = jsonTagsUtils.parseStrToCvEntityArthBean(entityResult);
                    } catch (Exception e) {
                        CV_ENTITY_LOG.info("cv_entity result parse to bean error" + e.getMessage());
                    }
                }
                arthTagsResumeBean.setCv_entity(cvEntityArthBean);
            } catch (Exception e) {
                CV_ENTITY_LOG.info("call cv_entity error," + e.getMessage());
            }
        }
        //cv_education学校&专业
        if (callAlgorithms.contains("cv_education")) {
            try {
                CV_EDUCATION_LOG.info("...call cv_education...");
                long start = System.currentTimeMillis();
                CvEducation cvEducation = new CvEducation(resumeId, originResumeBean);
                Future<String> submitEducation = CvEducation.getThreadPool().submit(cvEducation);
                String eduAndDegreeResult = submitEducation.get();
                eduResult = eduAndDegreeResult;
                long end = System.currentTimeMillis();
                CV_EDUCATION_LOG.info("cv_education result:" + eduAndDegreeResult + "]");
                CV_EDUCATION_LOG.info("cv_education use time:" + (end - start));
                LOGGER.info("cv_education use time:" + (end - start));
                String degree = "0";
                CvEducationArthBean cvEducationArthBean = new CvEducationArthBean();
                if (StringUtils.isNotBlank(eduAndDegreeResult)) {
                    try {
                        cvEducationArthBean = jsonTagsUtils.parseStrToCvEducationArthBean(eduAndDegreeResult);
                        CvEducationFeatures features = cvEducationArthBean.getFeatures();
                        if (null != features) {
                            degree = features.getDegree();
                        }
                    } catch (Exception e) {
                        CV_EDUCATION_LOG.info("cv_education result parse to bean error," + e.getMessage());
                    }
                }
                CV_EDUCATION_LOG.info("cv_degree result:[" + degree + "]");
                arthTagsResumeBean.setCv_education(cvEducationArthBean);
                int cvDegree = Integer.parseInt(degree);
                arthTagsResumeBean.setCv_degree(cvDegree);
                //如果basic中的degree数据不同于识别后的degree，那么就用识别后的进行更新
                int basicDegree = basic.getDegree();
                if (cvDegree != basicDegree) {
                    CV_EDUCATION_LOG.info("basic中的degree数据不同于识别后的degree");
                    basic.setDegree(cvDegree);
                    originResumeBean.setBasic(basic);
                }
            } catch (Exception e) {
                CV_EDUCATION_LOG.info("call cv_education error," + e.getMessage());
            }
        }
        //cv_feature特征提取
        if (callAlgorithms.contains("cv_feature")) {
            try {
                CV_FEATURE_LOG.info("...call cv_feature...");
                long start = System.currentTimeMillis();
                CvFeature cvFeature = new CvFeature(resumeId, originResumeBean);
                Future<String> submitFeature = CvFeature.getThreadPool().submit(cvFeature);
                String featureResult = submitFeature.get();
                long end = System.currentTimeMillis();
                CV_FEATURE_LOG.info("cv_feature result:[" + featureResult + "]");
                CV_FEATURE_LOG.info("cv_feature use time:" + (end - start));
                LOGGER.info("cv_feature use time:" + (end - start));
                CvFeatureArthBean cvFeatureArthBeans = new CvFeatureArthBean();
                if (StringUtils.isNotBlank(featureResult)) {
                    try {
                        cvFeatureArthBeans = jsonTagsUtils.parseStrToCvFeatureArthBean(featureResult);
                    } catch (Exception e) {
                        CV_FEATURE_LOG.info("cv_feature result parse to bean error," + e.getMessage());
                    }
                }
                arthTagsResumeBean.setCv_feature(cvFeatureArthBeans);
            } catch (Exception e) {
                CV_FEATURE_LOG.info("call cv_feature error," + e.getMessage());
            }
        }
        //cv_workyear工作年限
        if (callAlgorithms.contains("cv_workyear")) {
            try {
                CV_WORKYEAR_LOG.info("...call cv_workyear...");
                long start = System.currentTimeMillis();
                CvWexp cvWexp = new CvWexp(resumeId, originResumeBean);
                Future<String> submitWexp = CvWexp.getThreadPool().submit(cvWexp);
                wexpResult = submitWexp.get();
                long end = System.currentTimeMillis();
                CV_WORKYEAR_LOG.info("cv_workyear result:[" + wexpResult + "]");
                CV_WORKYEAR_LOG.info("cv_workyear use time:" + (end - start));
                LOGGER.info("cv_workyear use time:" + (end - start));
                int workYear = 0;
                try {
                    workYear = Integer.parseInt(wexpResult);
                } catch (NumberFormatException e) {
                    LOGGER.info(wexpResult + ",parse to int error");
                }
                arthTagsResumeBean.setCv_workyear(workYear);
            } catch (Exception e) {
                CV_WORKYEAR_LOG.info("call cv_workyear error," + e.getMessage());
            }
        }
        //cv_quality简历质量 依赖cv_title、cv_tag
        if (callAlgorithms.contains("cv_quality")) {
            try {
                CV_QUALITY_LOG.info("...call cv_quality...");
                long start = System.currentTimeMillis();
                int workExperience = 0;
                try {
                    workExperience = Integer.parseInt(wexpResult);
                } catch (NumberFormatException e) {
                    CV_QUALITY_LOG.info(wexpResult + "转成int报错," + e.getMessage());
                }
                CvQuality cvQuality = new CvQuality(resumeId, originResumeBean, titleResult, eduResult, tagResult, workExperience);
                Future<String> submit = CvQuality.getThreadPool().submit(cvQuality);
                String qualResult = submit.get();
                long end = System.currentTimeMillis();
                CV_QUALITY_LOG.info("cv_quality result:[" + qualResult + "]");
                CV_QUALITY_LOG.info("cv_quality use time:" + (end - start));
                LOGGER.info("cv_quality use time:" + (end - start));
                CvQualityArthBean cvQualityArthBeans = new CvQualityArthBean();
                if (StringUtils.isNotBlank(qualResult)) {
                    try {
                        cvQualityArthBeans = jsonTagsUtils.parseStrToCvQualityArthBean(qualResult);
                    } catch (Exception e) {
                        CV_QUALITY_LOG.info("cv_quality result parse to bean error," + e.getMessage());
                    }
                }
                arthTagsResumeBean.setCv_quality(cvQualityArthBeans);
            } catch (Exception e) {
                CV_QUALITY_LOG.info("call cv_quality error," + e.getMessage());
            }
        }
        //cv_language语言识别
        if (callAlgorithms.contains("cv_language")) {
            try {
                CV_LANGUAGE_LOG.info("...call cv_language...");
                long start = System.currentTimeMillis();
                CvLanguage cvLanguage = new CvLanguage(resumeId, originResumeBean);
                Future<String> submitLang = CvLanguage.getThreadPool().submit(cvLanguage);
                String langResult = submitLang.get();
                long end = System.currentTimeMillis();
                CV_LANGUAGE_LOG.info("cv_language result:[" + langResult + "]");
                CV_LANGUAGE_LOG.info("cv_language use time:" + (end - start));
                LOGGER.info("cv_language use time:" + (end - start));
                CvLanguageArthBean cvLanguageArthBean = new CvLanguageArthBean();
                if (StringUtils.isNotBlank(langResult)) {
                    try {
                        cvLanguageArthBean = jsonTagsUtils.parseStrToCvLanguageArthBean(langResult);
                    } catch (Exception e) {
                        CV_LANGUAGE_LOG.info("cv_language result parse to bean error," + e.getMessage());
                    }
                }
                arthTagsResumeBean.setCv_language(cvLanguageArthBean);
            } catch (Exception e) {
                CV_LANGUAGE_LOG.info("call cv_language error," + e.getMessage());
            }
        }
        //cv_resign离职率
        if (callAlgorithms.contains("cv_resign")) {
            try {
                CV_RESIGN_LOG.info("...call cv_resign...");
                long start = System.currentTimeMillis();
                CvResign cvResign = new CvResign(resumeId, originResumeBean, behavior, history);
                Future<String> submit = CvResign.getThreadPool().submit(cvResign);
                String resignResult = submit.get();
                long end = System.currentTimeMillis();
                CV_RESIGN_LOG.info("cv_resign result:[" + resignResult + "]");
                CV_RESIGN_LOG.info("cv_resign use time:" + (end - start));
                LOGGER.info("cv_resign use time:" + (end - start));
                Map<String, CvResignArthBean> cvResignMap = new HashMap<>();
                if (StringUtils.isNotBlank(resignResult)) {
                    try {
                        cvResignMap = jsonTagsUtils.parseStrToCvResignArthBean(resignResult);
                    } catch (Exception e) {
                        CV_RESIGN_LOG.info("cv_resign result parse to bean error," + e.getMessage());
                    }
                }
                arthTagsResumeBean.setCv_resign(cvResignMap);
            } catch (Exception e) {
                CV_RESIGN_LOG.info("call cv_resign error," + e.getMessage());
            }
        }
        //cv_certificate证书识别
        if (callAlgorithms.contains("cv_certificate")) {
            try {
                CV_CERTIFICATE_LOG.info("...call cv_certificate...");
                long start = System.currentTimeMillis();
                CvCertificate cvCertificate = new CvCertificate(resumeId, originResumeBean);
                Future<String> submitCertificate = CvCertificate.getThreadPool().submit(cvCertificate);
                String cerResult = submitCertificate.get();
                long end = System.currentTimeMillis();
                CV_CERTIFICATE_LOG.info("cv_certificate result:[" + cerResult + "]");
                CV_CERTIFICATE_LOG.info("cv_certificate use time:" + (end - start));
                LOGGER.info("cv_certificate use time:" + (end - start));
                Map<String, String> map = new HashMap<>();
                if (StringUtils.isNotBlank(cerResult)) {
                    try {
                        map = jsonTagsUtils.parseStrToCvCertificateArthBean(cerResult);
                    } catch (Exception e) {
                        CV_CERTIFICATE_LOG.info("cv_certificate result parse to bean error," + e.getMessage());
                    }
                }
                arthTagsResumeBean.setCv_certificate(map);
            } catch (Exception e) {
                CV_CERTIFICATE_LOG.info("call cv_certificate error," + e.getMessage());
            }
        }
        long arthEnd = System.currentTimeMillis();
        ARTH_LOG.info("{}:call algorithms use time:{}", resumeId, (arthEnd - arthStart));
        LOGGER.info(JSONObject.toJSONString(arthTagsResumeBean));
        resumeMessage.setArthTagsResumeBean(arthTagsResumeBean);
        return resumeMessage;
    }

}
