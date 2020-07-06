package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.msgpack.annotation.Message;

import java.io.Serializable;

@Message
public class CvEducationUnit implements Serializable {

    private String major;
    @JsonProperty("school_id")
    private long schoolId;
    @JsonProperty("major_explain")
    private String majorExplain;
    private String school;
    private int degree;
    @JsonProperty("major_id")
    private long majorId;
    @JsonProperty("school_explain")
    private String schoolExplain;

    public CvEducationUnit() {
    }

    public CvEducationUnit(String major, long schoolId, String majorExplain, String school, int degree, long majorId, String schoolExplain) {
        this.major = major;
        this.schoolId = schoolId;
        this.majorExplain = majorExplain;
        this.school = school;
        this.degree = degree;
        this.majorId = majorId;
        this.schoolExplain = schoolExplain;
    }


    public String getMajor() {
        return major;
    }

    public void setMajor(String major) {
        this.major = major;
    }

    public long getSchoolId() {
        return schoolId;
    }

    public void setSchoolId(long schoolId) {
        this.schoolId = schoolId;
    }

    public String getMajorExplain() {
        return majorExplain;
    }

    public void setMajorExplain(String majorExplain) {
        this.majorExplain = majorExplain;
    }

    public String getSchool() {
        return school;
    }

    public void setSchool(String school) {
        this.school = school;
    }

    public int getDegree() {
        return degree;
    }

    public void setDegree(int degree) {
        this.degree = degree;
    }

    public long getMajorId() {
        return majorId;
    }

    public void setMajorId(long majorId) {
        this.majorId = majorId;
    }

    public String getSchoolExplain() {
        return schoolExplain;
    }

    public void setSchoolExplain(String schoolExplain) {
        this.schoolExplain = schoolExplain;
    }
}
