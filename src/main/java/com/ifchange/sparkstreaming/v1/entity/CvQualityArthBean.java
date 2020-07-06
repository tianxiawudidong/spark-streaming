package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.msgpack.annotation.Message;

import java.io.Serializable;

@Message
public class CvQualityArthBean implements Serializable {
    @JsonProperty("totalcount")
    private int totalCount;

    private double score;
    @JsonProperty("score_detail")
    private String scoreDetail;

    public CvQualityArthBean(){}

    public CvQualityArthBean(int totalCount, double score, String scoreDetail) {
        this.totalCount = totalCount;
        this.score = score;
        this.scoreDetail = scoreDetail;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public String getScoreDetail() {
        return scoreDetail;
    }

    public void setScoreDetail(String scoreDetail) {
        this.scoreDetail = scoreDetail;
    }
}
