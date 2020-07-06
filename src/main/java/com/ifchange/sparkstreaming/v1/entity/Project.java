package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.msgpack.annotation.Message;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
@Message
public class Project implements Serializable {

    @JsonProperty("start_time")
    private String startTime;
    @JsonProperty("end_time")
    private String endTime;
    private String name;
    private String describe;
    private String responsibilities;
    @JsonProperty("so_far")
    private String soFar;
    private String achivement;
    @JsonProperty("position_name")
    private String positionName;
    @JsonProperty("corporation_name")
    private String corporationName;
    @JsonProperty("soft_env")
    private String softEnv;
    @JsonProperty("hard_env")
    private String hardEnv;
    @JsonProperty("develop_tool")
    private String developTool;
    private String id;
    @JsonProperty("is_deleted")
    private String isDeleted;
    @JsonProperty("sort_id")
    private String sortId;
    @JsonProperty("created_at")
    private String createdAt;
    @JsonProperty("updated_at")
    private String updatedAt;

    public Project(){}

    public Project(String startTime, String endTime, String name, String describe,
        String responsibilities, String soFar, String achivement, String positionName,
        String corporationName, String softEnv, String hardEnv, String developTool, String id,
        String isDeleted, String sortId, String createdAt, String updatedAt) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.name = name;
        this.describe = describe;
        this.responsibilities = responsibilities;
        this.soFar = soFar;
        this.achivement = achivement;
        this.positionName = positionName;
        this.corporationName = corporationName;
        this.softEnv = softEnv;
        this.hardEnv = hardEnv;
        this.developTool = developTool;
        this.id = id;
        this.isDeleted = isDeleted;
        this.sortId = sortId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescribe() {
        return describe;
    }

    public void setDescribe(String describe) {
        this.describe = describe;
    }

    public String getResponsibilities() {
        return responsibilities;
    }

    public void setResponsibilities(String responsibilities) {
        this.responsibilities = responsibilities;
    }

    public String getSoFar() {
        return soFar;
    }

    public void setSoFar(String soFar) {
        this.soFar = soFar;
    }

    public String getAchivement() {
        return achivement;
    }

    public void setAchivement(String achivement) {
        this.achivement = achivement;
    }

    public String getPositionName() {
        return positionName;
    }

    public void setPositionName(String positionName) {
        this.positionName = positionName;
    }

    public String getCorporationName() {
        return corporationName;
    }

    public void setCorporationName(String corporationName) {
        this.corporationName = corporationName;
    }

    public String getSoftEnv() {
        return softEnv;
    }

    public void setSoftEnv(String softEnv) {
        this.softEnv = softEnv;
    }

    public String getHardEnv() {
        return hardEnv;
    }

    public void setHardEnv(String hardEnv) {
        this.hardEnv = hardEnv;
    }

    public String getDevelopTool() {
        return developTool;
    }

    public void setDevelopTool(String developTool) {
        this.developTool = developTool;
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

    public String getSortId() {
        return sortId;
    }

    public void setSortId(String sortId) {
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
