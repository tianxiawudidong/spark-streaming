package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.msgpack.annotation.Message;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
@Message
public class Contact implements Serializable {

    private String name;
    private long phone;
    @JsonProperty("phone_area")
    private int phoneArea;
    private String email;
    private String qq;
    private String tel;
    private String sina;
    private String ten;
    private String msn;
    private String wechat;
    @JsonProperty("is_deleted")
    private String isDeleted;
    @JsonProperty("updated_at")
    private String updatedAt;
    @JsonProperty("created_at")
    private String createdAt;

    public Contact(){}

    public Contact(String name, long phone, int phoneArea, String email, String qq, String tel,
        String sina, String ten, String msn, String wechat, String isDeleted, String updatedAt,
        String createdAt) {
        this.name = name;
        this.phone = phone;
        this.phoneArea = phoneArea;
        this.email = email;
        this.qq = qq;
        this.tel = tel;
        this.sina = sina;
        this.ten = ten;
        this.msn = msn;
        this.wechat = wechat;
        this.isDeleted = isDeleted;
        this.updatedAt = updatedAt;
        this.createdAt = createdAt;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getPhone() {
        return phone;
    }

    public void setPhone(long phone) {
        this.phone = phone;
    }

    public int getPhoneArea() {
        return phoneArea;
    }

    public void setPhoneArea(int phoneArea) {
        this.phoneArea = phoneArea;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getQq() {
        return qq;
    }

    public void setQq(String qq) {
        this.qq = qq;
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getSina() {
        return sina;
    }

    public void setSina(String sina) {
        this.sina = sina;
    }

    public String getTen() {
        return ten;
    }

    public void setTen(String ten) {
        this.ten = ten;
    }

    public String getMsn() {
        return msn;
    }

    public void setMsn(String msn) {
        this.msn = msn;
    }

    public String getWechat() {
        return wechat;
    }

    public void setWechat(String wechat) {
        this.wechat = wechat;
    }

    public String getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(String isDeleted) {
        this.isDeleted = isDeleted;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }
}
