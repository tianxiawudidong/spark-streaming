package com.ifchange.sparkstreaming.v1.entity;

import org.msgpack.annotation.Message;

import java.io.Serializable;

/**
 * 算法结果bean
 * cv_title
 */
@Message
public class CvTitleArthBean implements Serializable{

    private String phrase;

    private int level;

    private long id;

    public CvTitleArthBean(){}

    public CvTitleArthBean(String phrase, int level, long id) {
        this.phrase = phrase;
        this.level = level;
        this.id = id;
    }

    public String getPhrase() {
        return phrase;
    }

    public void setPhrase(String phrase) {
        this.phrase = phrase;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}
