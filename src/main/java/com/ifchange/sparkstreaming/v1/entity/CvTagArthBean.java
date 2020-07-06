package com.ifchange.sparkstreaming.v1.entity;

import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.util.List;

@Message
public class CvTagArthBean implements Serializable {

    private List<String> should;

    private List<String> must;

    private String category;

    public CvTagArthBean(){}

    public CvTagArthBean(List<String> should, List<String> must, String category) {
        this.should = should;
        this.must = must;
        this.category = category;
    }

    public List<String> getShould() {
        return should;
    }

    public void setShould(List<String> should) {
        this.should = should;
    }

    public List<String> getMust() {
        return must;
    }

    public void setMust(List<String> must) {
        this.must = must;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }
}
