package com.ifchange.sparkstreaming.v1.entity;

import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.util.List;

@Message
public class CvLanguageArthCVS implements Serializable {

    private long id;

    private List<CvLanguageArthLanguage> language;

    public CvLanguageArthCVS() {
    }

    public CvLanguageArthCVS(long id, List<CvLanguageArthLanguage> language) {
        this.id = id;
        this.language = language;
    }

    public List<CvLanguageArthLanguage> getLanguage() {
        return language;
    }

    public void setLanguage(List<CvLanguageArthLanguage> language) {
        this.language = language;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

}
