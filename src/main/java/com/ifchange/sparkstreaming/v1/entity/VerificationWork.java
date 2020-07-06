package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.msgpack.annotation.Message;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
@Message
public class VerificationWork implements Serializable {
    @JsonProperty("start_time")
    private String startTime;
    @JsonProperty("end_time")
    private String endTime;
    @JsonProperty("corporation_name")
    private String corporationName;
    @JsonProperty("position_name")
    private String positionName;

    public VerificationWork(){}

    public VerificationWork(String startTime, String endTime, String corporationName, String positionName) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.corporationName = corporationName;
        this.positionName = positionName;
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

    public String getCorporationName() {
        return corporationName;
    }

    public void setCorporationName(String corporationName) {
        this.corporationName = corporationName;
    }

    public String getPositionName() {
        return positionName;
    }

    public void setPositionName(String positionName) {
        this.positionName = positionName;
    }

}
