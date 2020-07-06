package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@Message
public class CvTradeArthCompanyInfo implements Serializable {
    @JsonProperty("company_type")
    private List<String> companyType;

    @JsonProperty("internal_name")
    private String internalName;

    @JsonProperty("internal_id")
    private long internalId;

    private List<String> region;

    private List<String> keyword;

    public CvTradeArthCompanyInfo() {
    }

    public CvTradeArthCompanyInfo(List<String> companyType, String internalName, long internalId,
                                  List<String> region, List<String> keyword) {
        this.companyType = companyType;
        this.internalName = internalName;
        this.internalId = internalId;
        this.region = region;
        this.keyword = keyword;
    }


    public List<String> getCompanyType() {
        return companyType;
    }

    public void setCompanyType(List<String> companyType) {
        this.companyType = companyType;
    }

    public String getInternalName() {
        return internalName;
    }

    public void setInternalName(String internalName) {
        this.internalName = internalName;
    }

    public long getInternalId() {
        return internalId;
    }

    public void setInternalId(long internalId) {
        this.internalId = internalId;
    }

    public List<String> getRegion() {
        return region;
    }

    public void setRegion(List<String> region) {
        this.region = region;
    }

    public List<String> getKeyword() {
        return keyword;
    }

    public void setKeyword(List<String> keyword) {
        this.keyword = keyword;
    }

}
