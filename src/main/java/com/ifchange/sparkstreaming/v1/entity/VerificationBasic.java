package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.msgpack.annotation.Message;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
@Message
public class VerificationBasic implements Serializable {

    private String name;
    @JsonProperty("current_status")
    private String currentStatus;
    private String address;
    @JsonProperty("address_province")
    private String addressProvince;
    @JsonProperty("expect_salary_from")
    private String expectSalaryFrom;
    @JsonProperty("expect_salary_to")
    private String expectSalaryTo;
    @JsonProperty("expect_city_ids")
    private String expectCityIds;
    @JsonProperty("expect_remarks")
    private String expectRemarks;
    @JsonProperty("basic_salary_from")
    private String basicSalaryFrom;
    @JsonProperty("basic_salary_to")
    private String basicSalaryTo;

    VerificationBasic(){}

    public VerificationBasic(String name, String currentStatus, String address, String addressProvince,
        String expectSalaryFrom, String expectSalaryTo, String expectCityIds,
        String expectRemarks, String basicSalaryFrom, String basicSalaryTo) {
        this.name = name;
        this.currentStatus = currentStatus;
        this.address = address;
        this.addressProvince = addressProvince;
        this.expectSalaryFrom = expectSalaryFrom;
        this.expectSalaryTo = expectSalaryTo;
        this.expectCityIds = expectCityIds;
        this.expectRemarks = expectRemarks;
        this.basicSalaryFrom = basicSalaryFrom;
        this.basicSalaryTo = basicSalaryTo;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCurrentStatus() {
        return currentStatus;
    }

    public void setCurrentStatus(String currentStatus) {
        this.currentStatus = currentStatus;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getAddressProvince() {
        return addressProvince;
    }

    public void setAddressProvince(String addressProvince) {
        this.addressProvince = addressProvince;
    }

    public String getExpectSalaryFrom() {
        return expectSalaryFrom;
    }

    public void setExpectSalaryFrom(String expectSalaryFrom) {
        this.expectSalaryFrom = expectSalaryFrom;
    }

    public String getExpectSalaryTo() {
        return expectSalaryTo;
    }

    public void setExpectSalaryTo(String expectSalaryTo) {
        this.expectSalaryTo = expectSalaryTo;
    }

    public String getExpectCityIds() {
        return expectCityIds;
    }

    public void setExpectCityIds(String expectCityIds) {
        this.expectCityIds = expectCityIds;
    }

    public String getExpectRemarks() {
        return expectRemarks;
    }

    public void setExpectRemarks(String expectRemarks) {
        this.expectRemarks = expectRemarks;
    }

    public String getBasicSalaryFrom() {
        return basicSalaryFrom;
    }

    public void setBasicSalaryFrom(String basicSalaryFrom) {
        this.basicSalaryFrom = basicSalaryFrom;
    }

    public String getBasicSalaryTo() {
        return basicSalaryTo;
    }

    public void setBasicSalaryTo(String basicSalaryTo) {
        this.basicSalaryTo = basicSalaryTo;
    }

}
