package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@Message
public class Verification implements Serializable {

    private VerificationBasic basic;
    private VerificationContact contact;
    private Map<String, VerificationWork> work;

    public Verification(){}

    public Verification(VerificationBasic basic,
        VerificationContact contact,
        Map<String, VerificationWork> work) {
        this.basic = basic;
        this.contact = contact;
        this.work = work;
    }

    public VerificationBasic getBasic() {
        return basic;
    }

    public void setBasic(VerificationBasic basic) {
        this.basic = basic;
    }

    public VerificationContact getContact() {
        return contact;
    }

    public void setContact(VerificationContact contact) {
        this.contact = contact;
    }

    public Map<String, VerificationWork> getWork() {
        return work;
    }

    public void setWork(
        Map<String, VerificationWork> work) {
        this.work = work;
    }
}
