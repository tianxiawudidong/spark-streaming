package com.ifchange.sparkstreaming.v1.entity;

import org.msgpack.annotation.Message;

import java.io.Serializable;

@Message
public class CvEducationFeatures implements Serializable {

    private String degree;

    public CvEducationFeatures() {
    }

    public CvEducationFeatures(String degree) {
        this.degree = degree;
    }

    public String getDegree() {
        return degree;
    }

    public void setDegree(String degree) {
        this.degree = degree;
    }
}
