package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.msgpack.annotation.Message;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
@Message
public class Education implements Serializable {

    @JsonProperty("start_time")
    private String startTime;
    @JsonProperty("end_time")
    private String endTime;
    @JsonProperty("so_far")
    private String soFar;
    @JsonProperty("school_name")
    private String schoolName;
    @JsonProperty("discipline_name")
    private String disciplineName;
    private int degree;
    @JsonProperty("is_entrance")
    private String isEntrance;
    @JsonProperty("discipline_desc")
    private String disciplineDesc;
    private String id;
    @JsonProperty("is_deleted")
    private String isDeleted;
    @JsonProperty("sort_id")
    private int sortId;
    @JsonProperty("created_at")
    private String createdAt;
    @JsonProperty("updated_at")
    private String updatedAt;

    public Education(){}

    public Education(String startTime, String endTime, String soFar, String schoolName,
        String disciplineName, int degree, String isEntrance, String disciplineDesc,
        String id, String isDeleted, int sortId, String createdAt, String updatedAt) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.soFar = soFar;
        this.schoolName = schoolName;
        this.disciplineName = disciplineName;
        this.degree = degree;
        this.isEntrance = isEntrance;
        this.disciplineDesc = disciplineDesc;
        this.id = id;
        this.isDeleted = isDeleted;
        this.sortId = sortId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public String getSchoolName() {
        return schoolName;
    }

    public void setSchoolName(String schoolName) {
        this.schoolName = schoolName;
    }

    public String getDisciplineName() {
        return disciplineName;
    }

    public void setDisciplineName(String disciplineName) {
        this.disciplineName = disciplineName;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getSoFar() {
        return soFar;
    }

    public void setSoFar(String soFar) {
        this.soFar = soFar;
    }

    public int getDegree() {
        return degree;
    }

    public void setDegree(int degree) {
        this.degree = degree;
    }

    public String getIsEntrance() {
        return isEntrance;
    }

    public void setIsEntrance(String isEntrance) {
        this.isEntrance = isEntrance;
    }

    public String getDisciplineDesc() {
        return disciplineDesc;
    }

    public void setDisciplineDesc(String disciplineDesc) {
        this.disciplineDesc = disciplineDesc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(String isDeleted) {
        this.isDeleted = isDeleted;
    }

    public int getSortId() {
        return sortId;
    }

    public void setSortId(int sortId) {
        this.sortId = sortId;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }
}
