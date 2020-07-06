package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.util.List;

@Message
public class CvTradeArthBean implements Serializable {
    @JsonProperty("work_id")
    private String workId;
    @JsonProperty("company_id")
    private long companyId;
    @JsonProperty("company_name")
    private String companyName;
    @JsonProperty("company_info")
    private CvTradeArthCompanyInfo companyInfo;
    @JsonProperty("corp_cluster")
    private String corpCluster;
    @JsonProperty("first_trade_list")
    private List<String> firstTradeList;
    @JsonProperty("norm_parent_corp_id")
    private long normParentCorpId;
    @JsonProperty("second_trade_list")
    private List<String> secondTradeList;
    @JsonProperty("norm_corp_id")
    private long normCorpId;

    public CvTradeArthBean() {
    }

    public CvTradeArthBean(String workId, long companyId, String companyName,
                           CvTradeArthCompanyInfo companyInfo, String corpCluster, List<String> firstTradeList,
                           long normParentCorpId,
                           List<String> secondTradeList, long normCorpId) {
        this.workId = workId;
        this.companyId = companyId;
        this.companyName = companyName;
        this.companyInfo = companyInfo;
        this.corpCluster = corpCluster;
        this.firstTradeList = firstTradeList;
        this.normParentCorpId = normParentCorpId;
        this.secondTradeList = secondTradeList;
        this.normCorpId = normCorpId;
    }

    public String getWorkId() {
        return workId;
    }

    public void setWorkId(String workId) {
        this.workId = workId;
    }

    public long getCompanyId() {
        return companyId;
    }

    public void setCompanyId(long companyId) {
        this.companyId = companyId;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public CvTradeArthCompanyInfo getCompanyInfo() {
        return companyInfo;
    }

    public void setCompanyInfo(CvTradeArthCompanyInfo companyInfo) {
        this.companyInfo = companyInfo;
    }

    public String getCorpCluster() {
        return corpCluster;
    }

    public void setCorpCluster(String corpCluster) {
        this.corpCluster = corpCluster;
    }

    public List<String> getFirstTradeList() {
        return firstTradeList;
    }

    public void setFirstTradeList(List<String> firstTradeList) {
        this.firstTradeList = firstTradeList;
    }

    public long getNormParentCorpId() {
        return normParentCorpId;
    }

    public void setNormParentCorpId(long normParentCorpId) {
        this.normParentCorpId = normParentCorpId;
    }

    public List<String> getSecondTradeList() {
        return secondTradeList;
    }

    public void setSecondTradeList(List<String> secondTradeList) {
        this.secondTradeList = secondTradeList;
    }

    public long getNormCorpId() {
        return normCorpId;
    }

    public void setNormCorpId(long normCorpId) {
        this.normCorpId = normCorpId;
    }
}
