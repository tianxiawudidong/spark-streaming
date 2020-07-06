package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.msgpack.annotation.Message;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
@Message
public class VerificationContact implements Serializable {

    private String phone;

    public VerificationContact(){}

    public VerificationContact(String phone) {
        this.phone = phone;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

}
