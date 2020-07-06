package com.ifchange.sparkstreaming.v1.entity;

import org.msgpack.annotation.Message;

import java.io.Serializable;

/**
 * 简历带标签数据的消息格式:
 * 分为两部分：1. 简历原始数据格式。 2. 算法标签数据
 */
@Message
public class ResumeMessage implements Serializable {

    private OriginResumeBean originResumeBean;

    private ArthTagsResumeBean arthTagsResumeBean;

    public ResumeMessage() {
    }

    public ResumeMessage(OriginResumeBean originResumeBean,
                         ArthTagsResumeBean arthTagsResumeBean) {
        this.originResumeBean = originResumeBean;
        this.arthTagsResumeBean = arthTagsResumeBean;
    }

    public OriginResumeBean getOriginResumeBean() {
        return originResumeBean;
    }

    public void setOriginResumeBean(OriginResumeBean originResumeBean) {
        this.originResumeBean = originResumeBean;
    }

    public ArthTagsResumeBean getArthTagsResumeBean() {
        return arthTagsResumeBean;
    }

    public void setArthTagsResumeBean(
            ArthTagsResumeBean arthTagsResumeBean) {
        this.arthTagsResumeBean = arthTagsResumeBean;
    }
}
