package com.ifchange.sparkstreaming.v1.entity;

import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.util.List;

@Message
public class CvEntityArthBean implements Serializable {

    private List<String> must;

    private List<String> category;

    private CvEntityArthCommon title;

    private CvEntityArthCommon desc;

    private double version;

    public CvEntityArthBean(){}

    public CvEntityArthBean(List<String> must, List<String> category, CvEntityArthCommon title, CvEntityArthCommon desc, double version) {
        this.must = must;
        this.category = category;
        this.title = title;
        this.desc = desc;
        this.version = version;
    }

    public List<String> getMust() {
        return must;
    }

    public void setMust(List<String> must) {
        this.must = must;
    }

    public List<String> getCategory() {
        return category;
    }

    public void setCategory(List<String> category) {
        this.category = category;
    }

    public CvEntityArthCommon getTitle() {
        return title;
    }

    public void setTitle(CvEntityArthCommon title) {
        this.title = title;
    }

    public CvEntityArthCommon getDesc() {
        return desc;
    }

    public void setDesc(CvEntityArthCommon desc) {
        this.desc = desc;
    }

    public double getVersion() {
        return version;
    }

    public void setVersion(double version) {
        this.version = version;
    }
}


