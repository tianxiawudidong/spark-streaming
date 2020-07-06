package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.msgpack.annotation.Message;

import java.io.Serializable;

@Message
public class Training implements Serializable {

    @JsonProperty("start_time")
    private String startTime;
    @JsonProperty("end_time")
    private String endTime;
    @JsonProperty("so_far")
    private String soFar;
    private String name;
    private String city;
    private String authority;
    private String description;
    private String certificate;
    private String id;
    @JsonProperty("is_deleted")
    private String isDeleted;
    @JsonProperty("sort_id")
    private int sortId;
    @JsonProperty("created_at")
    private String createdAt;
    @JsonProperty("updated_at")
    private String updatedAt;

    public Training(){}

    public Training(String startTime, String endTime, String soFar, String name, String city,
        String authority, String description, String certificate, String id, String isDeleted,
        int sortId, String createdAt, String updatedAt) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.soFar = soFar;
        this.name = name;
        this.city = city;
        this.authority = authority;
        this.description = description;
        this.certificate = certificate;
        this.id = id;
        this.isDeleted = isDeleted;
        this.sortId = sortId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getAuthority() {
        return authority;
    }

    public void setAuthority(String authority) {
        this.authority = authority;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCertificate() {
        return certificate;
    }

    public void setCertificate(String certificate) {
        this.certificate = certificate;
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

}
