package com.ifchange.sparkstreaming.v1.entity;

import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.math.BigDecimal;

@Message
public class CvResignArthResignFeatures implements Serializable {

    private String name;

    private BigDecimal weight;

    public CvResignArthResignFeatures(){}

    public CvResignArthResignFeatures(String name, BigDecimal weight) {
        this.name = name;
        this.weight = weight;
    }

    public BigDecimal getWeight() {
        return weight;
    }

    public void setWeight(BigDecimal weight) {
        this.weight = weight;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


}
