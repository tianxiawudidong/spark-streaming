package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Message
public class CvFeatureArthWork implements Serializable {
    @JsonProperty("work_id")
    private String workId;
    @JsonProperty("start_time")
    private String startTime;
    @JsonProperty("end_time")
    private String endTime;
    private List<String> vector;
    private Map<String, String> title;
    @JsonProperty("title_entity")
    private List<String> titleEntity;
    @JsonProperty("desc_entity")
    private List<String> descEntity;

    private Map<String, String> desc;

    public CvFeatureArthWork() {
    }

    public CvFeatureArthWork(String workId, String startTime, String endTime, List<String> vector, Map<String, String> title, List<String> titleEntity, List<String> descEntity, Map<String, String> desc) {
        this.workId = workId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.vector = vector;
        this.title = title;
        this.titleEntity = titleEntity;
        this.descEntity = descEntity;
        this.desc = desc;
    }

    public String getWorkId() {
        return workId;
    }

    public void setWorkId(String workId) {
        this.workId = workId;
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

    public List<String> getVector() {
        return vector;
    }

    public void setVector(List<String> vector) {
        this.vector = vector;
    }

    public Map<String, String> getTitle() {
        return title;
    }

    public void setTitle(Map<String, String> title) {
        this.title = title;
    }

    public List<String> getTitleEntity() {
        return titleEntity;
    }

    public void setTitleEntity(List<String> titleEntity) {
        this.titleEntity = titleEntity;
    }

    public List<String> getDescEntity() {
        return descEntity;
    }

    public void setDescEntity(List<String> descEntity) {
        this.descEntity = descEntity;
    }

    public Map<String, String> getDesc() {
        return desc;
    }

    public void setDesc(Map<String, String> desc) {
        this.desc = desc;
    }
}
