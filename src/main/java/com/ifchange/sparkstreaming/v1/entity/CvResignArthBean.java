package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.util.Map;

@Message
public class CvResignArthBean implements Serializable {
    @JsonProperty("resign_features")
    private Map<String, CvResignArthResignFeatures> resignFeatures;
    @JsonProperty("resign_intention")
    private String resignIntention;

    private String history;

    public CvResignArthBean() {
    }

    public CvResignArthBean(
            Map<String, CvResignArthResignFeatures> resignFeatures, String resignIntention,
            String history) {
        this.resignFeatures = resignFeatures;
        this.resignIntention = resignIntention;
        this.history = history;
    }

    public Map<String, CvResignArthResignFeatures> getResignFeatures() {
        return resignFeatures;
    }

    public void setResignFeatures(
            Map<String, CvResignArthResignFeatures> resignFeatures) {
        this.resignFeatures = resignFeatures;
    }

    public String getResignIntention() {
        return resignIntention;
    }

    public void setResignIntention(String resignIntention) {
        this.resignIntention = resignIntention;
    }

    public String getHistory() {
        return history;
    }

    public void setHistory(String history) {
        this.history = history;
    }
}
