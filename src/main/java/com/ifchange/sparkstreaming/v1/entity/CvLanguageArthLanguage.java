package com.ifchange.sparkstreaming.v1.entity;

import org.msgpack.annotation.Message;

import java.io.Serializable;

@Message
public class CvLanguageArthLanguage implements Serializable {

    private String level;

    private String name;

    private String detail;

    public CvLanguageArthLanguage(){}

    public CvLanguageArthLanguage(String level, String name, String detail) {
        this.level = level;
        this.name = name;
        this.detail = detail;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }

}
