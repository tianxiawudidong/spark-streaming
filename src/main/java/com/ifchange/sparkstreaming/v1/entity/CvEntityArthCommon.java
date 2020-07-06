package com.ifchange.sparkstreaming.v1.entity;

import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.util.List;

@Message
public class CvEntityArthCommon implements Serializable {

    private List<String> major;

    private List<String> skill;

    public CvEntityArthCommon(){}

    public CvEntityArthCommon(List<String> major, List<String> skill) {
        this.major = major;
        this.skill = skill;
    }

    public List<String> getMajor() {
        return major;
    }

    public void setMajor(List<String> major) {
        this.major = major;
    }

    public List<String> getSkill() {
        return skill;
    }

    public void setSkill(List<String> skill) {
        this.skill = skill;
    }
}
