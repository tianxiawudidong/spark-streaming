package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.util.Map;

@Message
public class OriginResumeBean implements Serializable{

    @JsonProperty("resume_info")
    private ResumeInfo resumeInfo;
    private Map<String, Certificate> certificate;
    private Map<String, Education> education;
    private Map<String, Language> language;
    private Map<String, Project> project;
    private Map<String, Skill> skill;
    private Map<String, Training> training;
    private Map<String,Work> work;
    private Verification verification;
    private Contact contact;
    private Basic basic;

    public OriginResumeBean(){}

    public OriginResumeBean(ResumeInfo resumeInfo,
        Map<String, Certificate> certificate,
        Map<String, Education> education,
        Map<String, Language> language,
        Map<String, Project> project,
        Map<String, Skill> skill,
        Map<String, Training> training,
        Map<String, Work> work, Verification verification,
        Contact contact, Basic basic) {
        this.resumeInfo = resumeInfo;
        this.certificate = certificate;
        this.education = education;
        this.language = language;
        this.project = project;
        this.skill = skill;
        this.training = training;
        this.work = work;
        this.verification = verification;
        this.contact = contact;
        this.basic = basic;
    }

    public ResumeInfo getResumeInfo() {
        return resumeInfo;
    }

    public void setResumeInfo(ResumeInfo resumeInfo) {
        this.resumeInfo = resumeInfo;
    }

    public Map<String, Certificate> getCertificate() {
        return certificate;
    }

    public void setCertificate(
        Map<String, Certificate> certificate) {
        this.certificate = certificate;
    }

    public Map<String, Education> getEducation() {
        return education;
    }

    public void setEducation(
        Map<String, Education> education) {
        this.education = education;
    }

    public Map<String, Language> getLanguage() {
        return language;
    }

    public void setLanguage(
        Map<String, Language> language) {
        this.language = language;
    }

    public Map<String, Project> getProject() {
        return project;
    }

    public void setProject(
        Map<String, Project> project) {
        this.project = project;
    }

    public Map<String, Skill> getSkill() {
        return skill;
    }

    public void setSkill(Map<String, Skill> skill) {
        this.skill = skill;
    }

    public Map<String, Training> getTraining() {
        return training;
    }

    public void setTraining(
        Map<String, Training> training) {
        this.training = training;
    }

    public Map<String, Work> getWork() {
        return work;
    }

    public void setWork(Map<String, Work> work) {
        this.work = work;
    }

    public Verification getVerification() {
        return verification;
    }

    public void setVerification(Verification verification) {
        this.verification = verification;
    }

    public Contact getContact() {
        return contact;
    }

    public void setContact(Contact contact) {
        this.contact = contact;
    }

    public Basic getBasic() {
        return basic;
    }

    public void setBasic(Basic basic) {
        this.basic = basic;
    }
}
