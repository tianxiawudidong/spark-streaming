package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.msgpack.annotation.Message;

import java.io.Serializable;

@Message
@JsonIgnoreProperties(ignoreUnknown = true)
public class Work implements Serializable {

    @JsonProperty("start_time")
    private String startTime;
    @JsonProperty("end_time")
    private String endTime;
    @JsonProperty("so_far")
    private String soFar;
    @JsonProperty("corporation_name")
    private String corporationName;
    @JsonProperty("industry_name")
    private String industryName;
    @JsonProperty("architecture_name")
    private String architectureName;
    @JsonProperty("position_name")
    private String positionName;
    @JsonProperty("title_name")
    private String titleName;
    @JsonProperty("station_name")
    private String stationName;
    @JsonProperty("reporting_to")
    private String reportingTo;
    @JsonProperty("subordinates_count")
    private int subordinatesCount;
    private String responsibilities;
    @JsonProperty("management_experience")
    private String managementExperience;
    @JsonProperty("work_type")
    private String workType;
    @JsonProperty("basic_salary")
    private double basicSalary;
    private double bonus;
    @JsonProperty("annual_salary")
    private double annualSalary;
    @JsonProperty("basic_salary_from")
    private double basicSalaryFrom;
    @JsonProperty("basic_salary_to")
    private double basicSalaryTo;
    @JsonProperty("salary_month")
    private float salaryMonth;
    @JsonProperty("annual_salary_from")
    private double annualSalaryFrom;
    @JsonProperty("annual_salary_to")
    private double annualSalaryTo;
    @JsonProperty("corporation_desc")
    private String corporationDesc;
    private String scale;
    private String city;
    @JsonProperty("corporation_type")
    private String corporationType;
    private String reason;
    @JsonProperty("is_oversea")
    private String is_oversea;
    private String achievement;
    @JsonProperty("a_p_b")
    private String aPB;
    @JsonProperty("corporation_id")
    private int corporationId;
    @JsonProperty("industry_ids")
    private int industryIds;
    private String id;
    @JsonProperty("is_deleted")
    private String isDeleted;
    @JsonProperty("created_at")
    private String createdAt;
    @JsonProperty("updated_at")
    private String updatedAt;
    @JsonProperty("sort_id")
    private int sortId;
    @JsonProperty("title_category_id")
    private int titleCategoryId;
    @JsonProperty("architecture_id")
    private int architectureId;
    @JsonProperty("deleted_at")
    private int deletedAt;
    @JsonProperty("position_id")
    private int positionId;
    @JsonProperty("company_info")
    private CompanyInfo companyInfo;

    public Work(){}

    public Work(String startTime, String endTime, String soFar, String corporationName,
        String industryName, String architectureName, String positionName, String titleName,
        String stationName, String reportingTo, int subordinatesCount,
        String responsibilities, String managementExperience, String workType, double basicSalary,
        double bonus, double annualSalary, double basicSalaryFrom, double basicSalaryTo,
        float salaryMonth, double annualSalaryFrom, double annualSalaryTo,
        String corporationDesc, String scale, String city, String corporationType,
        String reason, String is_oversea, String achievement, String aPB, int corporationId,
        int industryIds, String id, String isDeleted, String createdAt, String updatedAt, int sortId,
        int titleCategoryId, int architectureId, int deletedAt, int positionId,
        CompanyInfo companyInfo) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.soFar = soFar;
        this.corporationName = corporationName;
        this.industryName = industryName;
        this.architectureName = architectureName;
        this.positionName = positionName;
        this.titleName = titleName;
        this.stationName = stationName;
        this.reportingTo = reportingTo;
        this.subordinatesCount = subordinatesCount;
        this.responsibilities = responsibilities;
        this.managementExperience = managementExperience;
        this.workType = workType;
        this.basicSalary = basicSalary;
        this.bonus = bonus;
        this.annualSalary = annualSalary;
        this.basicSalaryFrom = basicSalaryFrom;
        this.basicSalaryTo = basicSalaryTo;
        this.salaryMonth = salaryMonth;
        this.annualSalaryFrom = annualSalaryFrom;
        this.annualSalaryTo = annualSalaryTo;
        this.corporationDesc = corporationDesc;
        this.scale = scale;
        this.city = city;
        this.corporationType = corporationType;
        this.reason = reason;
        this.is_oversea = is_oversea;
        this.achievement = achievement;
        this.aPB = aPB;
        this.corporationId = corporationId;
        this.industryIds = industryIds;
        this.id = id;
        this.isDeleted = isDeleted;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.sortId = sortId;
        this.titleCategoryId = titleCategoryId;
        this.architectureId = architectureId;
        this.deletedAt = deletedAt;
        this.positionId = positionId;
        this.companyInfo = companyInfo;
    }

    public CompanyInfo getCompanyInfo() {
        return companyInfo;
    }

    public void setCompanyInfo(CompanyInfo companyInfo) {
        this.companyInfo = companyInfo;
    }

    public int getPositionId() {
        return positionId;
    }

    public void setPositionId(int positionId) {
        this.positionId = positionId;
    }

    public int getDeletedAt() {
        return deletedAt;
    }

    public void setDeletedAt(int deletedAt) {
        this.deletedAt = deletedAt;
    }



    public int getTitleCategoryId() {
        return titleCategoryId;
    }

    public void setTitleCategoryId(int titleCategoryId) {
        this.titleCategoryId = titleCategoryId;
    }

    public int getArchitectureId() {
        return architectureId;
    }

    public void setArchitectureId(int architectureId) {
        this.architectureId = architectureId;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getSoFar() {
        return soFar;
    }

    public void setSoFar(String soFar) {
        this.soFar = soFar;
    }

    public String getCorporationName() {
        return corporationName;
    }

    public void setCorporationName(String corporationName) {
        this.corporationName = corporationName;
    }

    public String getIndustryName() {
        return industryName;
    }

    public void setIndustryName(String industryName) {
        this.industryName = industryName;
    }

    public String getArchitectureName() {
        return architectureName;
    }

    public void setArchitectureName(String architectureName) {
        this.architectureName = architectureName;
    }

    public String getPositionName() {
        return positionName;
    }

    public void setPositionName(String positionName) {
        this.positionName = positionName;
    }

    public String getTitleName() {
        return titleName;
    }

    public void setTitleName(String titleName) {
        this.titleName = titleName;
    }

    public String getStationName() {
        return stationName;
    }

    public void setStationName(String stationName) {
        this.stationName = stationName;
    }

    public String getReportingTo() {
        return reportingTo;
    }

    public void setReportingTo(String reportingTo) {
        this.reportingTo = reportingTo;
    }

    public int getSubordinatesCount() {
        return subordinatesCount;
    }

    public void setSubordinatesCount(int subordinatesCount) {
        this.subordinatesCount = subordinatesCount;
    }

    public String getResponsibilities() {
        return responsibilities;
    }

    public void setResponsibilities(String responsibilities) {
        this.responsibilities = responsibilities;
    }

    public String getManagementExperience() {
        return managementExperience;
    }

    public void setManagementExperience(String managementExperience) {
        this.managementExperience = managementExperience;
    }

    public String getWorkType() {
        return workType;
    }

    public void setWorkType(String workType) {
        this.workType = workType;
    }

    public double getBasicSalary() {
        return basicSalary;
    }

    public void setBasicSalary(double basicSalary) {
        this.basicSalary = basicSalary;
    }

    public double getBonus() {
        return bonus;
    }

    public void setBonus(double bonus) {
        this.bonus = bonus;
    }

    public double getAnnualSalary() {
        return annualSalary;
    }

    public void setAnnualSalary(double annualSalary) {
        this.annualSalary = annualSalary;
    }

    public double getBasicSalaryFrom() {
        return basicSalaryFrom;
    }

    public void setBasicSalaryFrom(double basicSalaryFrom) {
        this.basicSalaryFrom = basicSalaryFrom;
    }

    public double getBasicSalaryTo() {
        return basicSalaryTo;
    }

    public void setBasicSalaryTo(double basicSalaryTo) {
        this.basicSalaryTo = basicSalaryTo;
    }

    public float getSalaryMonth() {
        return salaryMonth;
    }

    public void setSalaryMonth(float salaryMonth) {
        this.salaryMonth = salaryMonth;
    }

    public double getAnnualSalaryFrom() {
        return annualSalaryFrom;
    }

    public void setAnnualSalaryFrom(double annualSalaryFrom) {
        this.annualSalaryFrom = annualSalaryFrom;
    }

    public double getAnnualSalaryTo() {
        return annualSalaryTo;
    }

    public void setAnnualSalaryTo(double annualSalaryTo) {
        this.annualSalaryTo = annualSalaryTo;
    }

    public String getCorporationDesc() {
        return corporationDesc;
    }

    public void setCorporationDesc(String corporationDesc) {
        this.corporationDesc = corporationDesc;
    }

    public String getScale() {
        return scale;
    }

    public void setScale(String scale) {
        this.scale = scale;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCorporationType() {
        return corporationType;
    }

    public void setCorporationType(String corporationType) {
        this.corporationType = corporationType;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getIs_oversea() {
        return is_oversea;
    }

    public void setIs_oversea(String is_oversea) {
        this.is_oversea = is_oversea;
    }

    public String getAchievement() {
        return achievement;
    }

    public void setAchievement(String achievement) {
        this.achievement = achievement;
    }

    public String getaPB() {
        return aPB;
    }

    public void setaPB(String aPB) {
        this.aPB = aPB;
    }

    public int getCorporationId() {
        return corporationId;
    }

    public void setCorporationId(int corporationId) {
        this.corporationId = corporationId;
    }

    public int getIndustryIds() {
        return industryIds;
    }

    public void setIndustryIds(int industryIds) {
        this.industryIds = industryIds;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(String isDeleted) {
        this.isDeleted = isDeleted;
    }

    public int getSortId() {
        return sortId;
    }

    public void setSortId(int sortId) {
        this.sortId = sortId;
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
