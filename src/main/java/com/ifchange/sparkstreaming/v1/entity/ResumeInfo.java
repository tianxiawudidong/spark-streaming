package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.msgpack.annotation.Message;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
@Message
public class ResumeInfo implements Serializable {

    private long id;
    @JsonProperty("contact_id")
    private long contactId;
    @JsonProperty("has_phone")
    private String hasPhone;
    @JsonProperty("attachment_ids")
    private String attachmentIds;
    @JsonProperty("resume_updated_at")
    private String resumeUpdatedAt;
    @JsonProperty("user_id")
    private long userId;
    private String name;
    @JsonProperty("resume_name")
    private String resumeName;
    @JsonProperty("industry_ids")
    private String industryIds;
    @JsonProperty("work_experience")
    private int workExperience;
    @JsonProperty("is_validate")
    private String isValidate;
    @JsonProperty("is_increased")
    private String isIncreased;
    @JsonProperty("is_private")
    private int isPrivate;
    @JsonProperty("is_processing")
    private int isProcessing;
    @JsonProperty("is_deleted")
    private String isDeleted;
    @JsonProperty("created_at")
    private String createdAt;
    @JsonProperty("updated_at")
    private String updatedAt;

    public ResumeInfo() {
    }

    public ResumeInfo(long id, long contactId, String hasPhone, String attachmentIds,
                      String resumeUpdatedAt, long userId, String name, String resumeName, String industryIds,
                      int workExperience, String isValidate, String isIncreased, int isPrivate, int isProcessing,
                      String isDeleted, String createdAt, String updatedAt) {
        this.id = id;
        this.contactId = contactId;
        this.hasPhone = hasPhone;
        this.attachmentIds = attachmentIds;
        this.resumeUpdatedAt = resumeUpdatedAt;
        this.userId = userId;
        this.name = name;
        this.resumeName = resumeName;
        this.industryIds = industryIds;
        this.workExperience = workExperience;
        this.isValidate = isValidate;
        this.isIncreased = isIncreased;
        this.isPrivate = isPrivate;
        this.isProcessing = isProcessing;
        this.isDeleted = isDeleted;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getContactId() {
        return contactId;
    }

    public void setContactId(long contactId) {
        this.contactId = contactId;
    }

    public String getHasPhone() {
        return hasPhone;
    }

    public void setHasPhone(String hasPhone) {
        this.hasPhone = hasPhone;
    }

    public String getAttachmentIds() {
        return attachmentIds;
    }

    public void setAttachmentIds(String attachmentIds) {
        this.attachmentIds = attachmentIds;
    }

    public String getResumeUpdatedAt() {
        return resumeUpdatedAt;
    }

    public void setResumeUpdatedAt(String resumeUpdatedAt) {
        this.resumeUpdatedAt = resumeUpdatedAt;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getResumeName() {
        return resumeName;
    }

    public void setResumeName(String resumeName) {
        this.resumeName = resumeName;
    }

    public String getIndustryIds() {
        return industryIds;
    }

    public void setIndustryIds(String industryIds) {
        this.industryIds = industryIds;
    }

    public int getWorkExperience() {
        return workExperience;
    }

    public void setWorkExperience(int workExperience) {
        this.workExperience = workExperience;
    }

    public String getIsValidate() {
        return isValidate;
    }

    public void setIsValidate(String isValidate) {
        this.isValidate = isValidate;
    }

    public String getIsIncreased() {
        return isIncreased;
    }

    public void setIsIncreased(String isIncreased) {
        this.isIncreased = isIncreased;
    }

    public int getIsPrivate() {
        return isPrivate;
    }

    public void setIsPrivate(int isPrivate) {
        this.isPrivate = isPrivate;
    }

    public int getIsProcessing() {
        return isProcessing;
    }

    public void setIsProcessing(int isProcessing) {
        this.isProcessing = isProcessing;
    }

    public String getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(String isDeleted) {
        this.isDeleted = isDeleted;
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
