package com.ifchange.sparkstreaming.v1.entity;

import org.msgpack.annotation.Message;

import java.io.Serializable;

@Message
public class CvDegreeArthBean implements Serializable {

    private int degree;

    public CvDegreeArthBean(){}

    public CvDegreeArthBean(int degree) {
        this.degree = degree;
    }

    public int getDegree() {
        return degree;
    }

    public void setDegree(int degree) {
        this.degree = degree;
    }
}
