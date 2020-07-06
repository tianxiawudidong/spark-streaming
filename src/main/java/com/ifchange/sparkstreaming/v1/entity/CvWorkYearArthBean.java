package com.ifchange.sparkstreaming.v1.entity;

import org.msgpack.annotation.Message;

import java.io.Serializable;

@Message
public class CvWorkYearArthBean implements Serializable{

    private int workExperience;

    public CvWorkYearArthBean(){}

    public CvWorkYearArthBean(int workExperience) {
        this.workExperience = workExperience;
    }

    public int getWorkExperience() {
        return workExperience;
    }

    public void setWorkExperience(int workExperience) {
        this.workExperience = workExperience;
    }
}
