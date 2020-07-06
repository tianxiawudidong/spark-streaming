package com.ifchange.sparkstreaming.v1.entity;

import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.util.List;

@Message
public class CvLanguageArthBean implements Serializable {

    private List<CvLanguageArthCVS> cvs;

    public CvLanguageArthBean(){}

    public CvLanguageArthBean(List<CvLanguageArthCVS> cvs) {
        this.cvs = cvs;
    }

    public List<CvLanguageArthCVS> getCvs() {
        return cvs;
    }

    public void setCvs(List<CvLanguageArthCVS> cvs) {
        this.cvs = cvs;
    }
}
