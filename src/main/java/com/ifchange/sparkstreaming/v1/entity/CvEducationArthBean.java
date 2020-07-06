package com.ifchange.sparkstreaming.v1.entity;

import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.util.Map;

@Message
public class CvEducationArthBean implements Serializable {

    private CvEducationFeatures features;

    private Map<String, CvEducationUnit> units;

    public CvEducationArthBean(CvEducationFeatures features, Map<String, CvEducationUnit> units) {
        this.features = features;
        this.units = units;
    }

    public CvEducationArthBean() {
    }

    public CvEducationFeatures getFeatures() {
        return features;
    }

    public void setFeatures(CvEducationFeatures features) {
        this.features = features;
    }

    public Map<String, CvEducationUnit> getUnits() {
        return units;
    }

    public void setUnits(Map<String, CvEducationUnit> units) {
        this.units = units;
    }
}
