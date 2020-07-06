package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.util.List;

@Message
public class CompanyInfo implements Serializable {

    @JsonProperty("company_type")
    private List<String> companyType;
    @JsonProperty("region")
    private List<String> region;
    @JsonProperty("keyword")
    private List<String> keyword;

    public CompanyInfo(){}

    public CompanyInfo(List<String> companyType, List<String> region,
        List<String> keyword) {
        this.companyType = companyType;
        this.region = region;
        this.keyword = keyword;
    }

    public List<String> getCompanyType() {
        return companyType;
    }

    public void setCompanyType(List<String> companyType) {
        this.companyType = companyType;
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
