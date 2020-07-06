package com.ifchange.sparkstreaming.v1.entity;

import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Message
public class CvCertificateArthBean implements Serializable {

    private Map<String, List<Integer>> certificateIds;

    public CvCertificateArthBean(){}

    public CvCertificateArthBean(
        Map<String, List<Integer>> certificateIds) {
        this.certificateIds = certificateIds;
    }

    public Map<String, List<Integer>> getCertificateIds() {
        return certificateIds;
    }

    public void setCertificateIds(
        Map<String, List<Integer>> certificateIds) {
        this.certificateIds = certificateIds;
    }
}
