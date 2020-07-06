package com.ifchange.sparkstreaming.v1.entity;

import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Message
public class ArthTagsResumeBean implements Serializable {

    private List<CvTradeArthBean> cv_trade;
    private Map<String, CvTitleArthBean> cv_title;
    private Map<String, CvTagArthBean> cv_tag;
    private Map<String, CvEntityArthBean> cv_entity;
    private CvEducationArthBean cv_education;
    private CvFeatureArthBean cv_feature;
    private CvQualityArthBean cv_quality;
    private CvLanguageArthBean cv_language;
    private Map<String, CvResignArthBean> cv_resign;
    private int cv_workyear;
    private Map<String, String> cv_certificate;
    private int cv_degree;
    private String cv_source;
    private int cv_current_status;

    public ArthTagsResumeBean() {
    }

    public ArthTagsResumeBean(List<CvTradeArthBean> cv_trade, Map<String, CvTitleArthBean> cv_title, Map<String, CvTagArthBean> cv_tag, Map<String, CvEntityArthBean> cv_entity, CvEducationArthBean cv_education, CvFeatureArthBean cv_feature, CvQualityArthBean cv_quality, CvLanguageArthBean cv_language, Map<String, CvResignArthBean> cv_resign, int cv_workyear, Map<String, String> cv_certificate, int cv_degree, String cv_source, int cv_current_status) {
        this.cv_trade = cv_trade;
        this.cv_title = cv_title;
        this.cv_tag = cv_tag;
        this.cv_entity = cv_entity;
        this.cv_education = cv_education;
        this.cv_feature = cv_feature;
        this.cv_quality = cv_quality;
        this.cv_language = cv_language;
        this.cv_resign = cv_resign;
        this.cv_workyear = cv_workyear;
        this.cv_certificate = cv_certificate;
        this.cv_degree = cv_degree;
        this.cv_source = cv_source;
        this.cv_current_status = cv_current_status;
    }

    public List<CvTradeArthBean> getCv_trade() {
        return cv_trade;
    }

    public void setCv_trade(List<CvTradeArthBean> cv_trade) {
        this.cv_trade = cv_trade;
    }

    public Map<String, CvTitleArthBean> getCv_title() {
        return cv_title;
    }

    public void setCv_title(Map<String, CvTitleArthBean> cv_title) {
        this.cv_title = cv_title;
    }

    public Map<String, CvTagArthBean> getCv_tag() {
        return cv_tag;
    }

    public void setCv_tag(Map<String, CvTagArthBean> cv_tag) {
        this.cv_tag = cv_tag;
    }

    public Map<String, CvEntityArthBean> getCv_entity() {
        return cv_entity;
    }

    public void setCv_entity(Map<String, CvEntityArthBean> cv_entity) {
        this.cv_entity = cv_entity;
    }

    public CvEducationArthBean getCv_education() {
        return cv_education;
    }

    public void setCv_education(CvEducationArthBean cv_education) {
        this.cv_education = cv_education;
    }

    public CvFeatureArthBean getCv_feature() {
        return cv_feature;
    }

    public void setCv_feature(CvFeatureArthBean cv_feature) {
        this.cv_feature = cv_feature;
    }

    public CvQualityArthBean getCv_quality() {
        return cv_quality;
    }

    public void setCv_quality(CvQualityArthBean cv_quality) {
        this.cv_quality = cv_quality;
    }

    public CvLanguageArthBean getCv_language() {
        return cv_language;
    }

    public void setCv_language(CvLanguageArthBean cv_language) {
        this.cv_language = cv_language;
    }

    public Map<String, CvResignArthBean> getCv_resign() {
        return cv_resign;
    }

    public void setCv_resign(Map<String, CvResignArthBean> cv_resign) {
        this.cv_resign = cv_resign;
    }

    public int getCv_workyear() {
        return cv_workyear;
    }

    public void setCv_workyear(int cv_workyear) {
        this.cv_workyear = cv_workyear;
    }

    public Map<String, String> getCv_certificate() {
        return cv_certificate;
    }

    public void setCv_certificate(Map<String, String> cv_certificate) {
        this.cv_certificate = cv_certificate;
    }

    public int getCv_degree() {
        return cv_degree;
    }

    public void setCv_degree(int cv_degree) {
        this.cv_degree = cv_degree;
    }

    public String getCv_source() {
        return cv_source;
    }

    public void setCv_source(String cv_source) {
        this.cv_source = cv_source;
    }

    public int getCv_current_status() {
        return cv_current_status;
    }

    public void setCv_current_status(int cv_current_status) {
        this.cv_current_status = cv_current_status;
    }
}
