package com.ifchange.sparkstreaming.v1.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@Message
public class Basic implements Serializable {

    private int id;
    @JsonProperty("contact_id")
    private int contactId;
    @JsonProperty("resume_updated_at")
    private String resumeUpdatedAt;
    private String name;
    @JsonProperty("resume_name")
    private String resumeName;
    @JsonProperty("industry_ids")
    private String industryIds;
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
    @JsonProperty("updated_at")
    private String updatedAt;
    @JsonProperty("created_at")
    private String createdAt;
    private String gender;
    private String birth;
    private String nation;
    private String marital;
    @JsonProperty("is_fertility")
    private String isFertility;
    @JsonProperty("is_house")
    private String isHouse;
    @JsonProperty("live_family")
    private String liveFamily;
    @JsonProperty("account_province")
    private int accountProvince;
    private int account;
    @JsonProperty("native_place_province")
    private int nativePlaceProvince;
    @JsonProperty("native_place")
    private int nativePlace;
    @JsonProperty("address_province")
    private int addressProvince;
    private int address;
    @JsonProperty("current_status")
    private int currentStatus;
    private int src;
    @JsonProperty("src_no")
    private String srcNo;
    @JsonProperty("expect_city_ids")
    private String expectCityIds;
    @JsonProperty("expect_city_names")
    private String expectCityNames;
    @JsonProperty("expect_salary_from")
    private double expectSalaryFrom;
    @JsonProperty("expect_salary_to")
    private double expectSalaryTo;
    @JsonProperty("expect_salary_month")
    private float expectSalaryMonth;
    @JsonProperty("expect_annual_salary_from")
    private double expectAnnualSalaryFrom;
    @JsonProperty("expect_annual_salary_to")
    private double expectAnnualSalaryTo;
    @JsonProperty("expect_annual_salary")
    private double expectAnnualSalary;
    @JsonProperty("expect_bonus")
    private double expectBonus;
    @JsonProperty("expect_work_at")
    private String expectWorkAt;
    @JsonProperty("expect_type")
    private String expectType;
    @JsonProperty("expect_position_name")
    private String expectPositionName;
    @JsonProperty("expect_industry_name")
    private String expectIndustryName;
    @JsonProperty("expect_remarks")
    private String expectRemarks;
    @JsonProperty("not_expect_corporation_name")
    private String notExpectCorporationName;
    @JsonProperty("not_expect_corporation_ids")
    private String notExpectCorporationIds;
    @JsonProperty("not_expect_corporation_status")
    private int notExpectCorporationStatus;
    @JsonProperty("is_active")
    private String isActive;
    @JsonProperty("bonus_text")
    private String bonusText;
    private String options;
    @JsonProperty("other_welfare")
    private String otherWelfare;
    private String interests;
    private String overseas;
    @JsonProperty("political_status")
    private String politicalStatus;
    @JsonProperty("project_info")
    private String projectInfo;
    @JsonProperty("other_info")
    private String otherInfo;
    private String card;
    private String speciality;
    @JsonProperty("phone_id")
    private String phoneId;
    @JsonProperty("mail_id")
    private String mailId;
    @JsonProperty("nation_id")
    private int nationId;
    private String photo;
    @JsonProperty("self_remark")
    private String selfRemark;
    @JsonProperty("focused_corporations")
    //private String focusedCorporations;
    private List<String> focusedCorporations;
    @JsonProperty("focused_feelings")
    //private String focusedFeelings;
    private List<String> focusedFeelings;
    @JsonProperty("is_core")
    private String isCore;
    private String customization;
    private String status;
    @JsonProperty("is_add_v")
    private String isAddV;
    @JsonProperty("locked_time")
    private int lockedTime;
    @JsonProperty("is_author")
    private String isAuthor;
    @JsonProperty("verification_at")
    private int verificationAt;
    private String practice;
    private String study;
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
    private String isOversea;
    private String achievement;
    @JsonProperty("a_p_b")
    private String aPB;
    @JsonProperty("corporation_id")
    private int corporationId;
    @JsonProperty("school_name")
    private String schoolName;
    @JsonProperty("discipline_name")
    private String disciplineName;
    private int degree;
    @JsonProperty("is_entrance")
    private String isEntrance;
    @JsonProperty("discipline_desc")
    private String disciplineDesc;
    @JsonProperty("internal_title_name")
    private String internalTitleName;
    @JsonProperty("section")
    private String section;

    @JsonProperty("company_info")
    private CompanyInfo companyInfo;

    @JsonProperty("title_id")
    private String titleId;
    @JsonProperty("my_project")
    private String myProject;
    private String zip;
    @JsonProperty("address_detail")
    private String addressDetail;
    @JsonProperty("school_id")
    private String schoolId;
    @JsonProperty("internal_title_id")
    private String internalTitleId;
    @JsonProperty("work_experience")
    private String workExperience;
    @JsonProperty("driver_license")
    private String driverLicense;
    @JsonProperty("actived_at")
    private String activedAt;
    private String passport;
    private String remark;
    @JsonProperty("architecture_id")
    private String architectureId;
    private String homepage;
    private String category;

    public Basic(){}

    public Basic(int id, int contactId, String resumeUpdatedAt, String name, String resumeName,
        String industryIds, String isValidate, String isIncreased, int isPrivate, int isProcessing,
        String isDeleted, String updatedAt, String createdAt, String gender, String birth,
        String nation, String marital, String isFertility, String isHouse, String liveFamily,
        int accountProvince, int account, int nativePlaceProvince, int nativePlace,
        int addressProvince,
        int address, int currentStatus, int src, String srcNo, String expectCityIds,
        String expectCityNames, double expectSalaryFrom, double expectSalaryTo,
        float expectSalaryMonth,
        double expectAnnualSalaryFrom, double expectAnnualSalaryTo, double expectAnnualSalary,
        double expectBonus, String expectWorkAt, String expectType, String expectPositionName,
        String expectIndustryName, String expectRemarks, String notExpectCorporationName,
        String notExpectCorporationIds, int notExpectCorporationStatus, String isActive,
        String bonusText, String options, String otherWelfare, String interests,
        String overseas, String politicalStatus, String projectInfo, String otherInfo,
        String card, String speciality, String phoneId, String mailId, int nationId,
        String photo, String selfRemark, List<String> focusedCorporations, List<String> focusedFeelings,
        String isCore, String customization, String status, String isAddV, int lockedTime,
        String isAuthor, int verificationAt, String practice, String study, String startTime,
        String endTime, String soFar, String corporationName, String industryName,
        String architectureName, String positionName, String titleName, String stationName,
        String reportingTo, int subordinatesCount, String responsibilities,
        String managementExperience, String workType, double basicSalary, double bonus,
        double annualSalary, double basicSalaryFrom, double basicSalaryTo, float salaryMonth,
        double annualSalaryFrom, double annualSalaryTo, String corporationDesc, String scale,
        String city, String corporationType, String reason, String isOversea,
        String achievement, String aPB, int corporationId, String schoolName,
        String disciplineName, int degree, String isEntrance, String disciplineDesc,
        String internalTitleName, String section,
        CompanyInfo companyInfo, String titleId, String myProject, String zip,
        String addressDetail, String schoolId, String internalTitleId, String workExperience,
        String driverLicense, String activedAt, String passport, String remark,
        String architectureId, String homepage, String category) {
        this.id = id;
        this.contactId = contactId;
        this.resumeUpdatedAt = resumeUpdatedAt;
        this.name = name;
        this.resumeName = resumeName;
        this.industryIds = industryIds;
        this.isValidate = isValidate;
        this.isIncreased = isIncreased;
        this.isPrivate = isPrivate;
        this.isProcessing = isProcessing;
        this.isDeleted = isDeleted;
        this.updatedAt = updatedAt;
        this.createdAt = createdAt;
        this.gender = gender;
        this.birth = birth;
        this.nation = nation;
        this.marital = marital;
        this.isFertility = isFertility;
        this.isHouse = isHouse;
        this.liveFamily = liveFamily;
        this.accountProvince = accountProvince;
        this.account = account;
        this.nativePlaceProvince = nativePlaceProvince;
        this.nativePlace = nativePlace;
        this.addressProvince = addressProvince;
        this.address = address;
        this.currentStatus = currentStatus;
        this.src = src;
        this.srcNo = srcNo;
        this.expectCityIds = expectCityIds;
        this.expectCityNames = expectCityNames;
        this.expectSalaryFrom = expectSalaryFrom;
        this.expectSalaryTo = expectSalaryTo;
        this.expectSalaryMonth = expectSalaryMonth;
        this.expectAnnualSalaryFrom = expectAnnualSalaryFrom;
        this.expectAnnualSalaryTo = expectAnnualSalaryTo;
        this.expectAnnualSalary = expectAnnualSalary;
        this.expectBonus = expectBonus;
        this.expectWorkAt = expectWorkAt;
        this.expectType = expectType;
        this.expectPositionName = expectPositionName;
        this.expectIndustryName = expectIndustryName;
        this.expectRemarks = expectRemarks;
        this.notExpectCorporationName = notExpectCorporationName;
        this.notExpectCorporationIds = notExpectCorporationIds;
        this.notExpectCorporationStatus = notExpectCorporationStatus;
        this.isActive = isActive;
        this.bonusText = bonusText;
        this.options = options;
        this.otherWelfare = otherWelfare;
        this.interests = interests;
        this.overseas = overseas;
        this.politicalStatus = politicalStatus;
        this.projectInfo = projectInfo;
        this.otherInfo = otherInfo;
        this.card = card;
        this.speciality = speciality;
        this.phoneId = phoneId;
        this.mailId = mailId;
        this.nationId = nationId;
        this.photo = photo;
        this.selfRemark = selfRemark;
        this.focusedCorporations = focusedCorporations;
        this.focusedFeelings = focusedFeelings;
        this.isCore = isCore;
        this.customization = customization;
        this.status = status;
        this.isAddV = isAddV;
        this.lockedTime = lockedTime;
        this.isAuthor = isAuthor;
        this.verificationAt = verificationAt;
        this.practice = practice;
        this.study = study;
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
        this.isOversea = isOversea;
        this.achievement = achievement;
        this.aPB = aPB;
        this.corporationId = corporationId;
        this.schoolName = schoolName;
        this.disciplineName = disciplineName;
        this.degree = degree;
        this.isEntrance = isEntrance;
        this.disciplineDesc = disciplineDesc;
        this.internalTitleName = internalTitleName;
        this.section = section;
        this.companyInfo = companyInfo;
        this.titleId = titleId;
        this.myProject = myProject;
        this.zip = zip;
        this.addressDetail = addressDetail;
        this.schoolId = schoolId;
        this.internalTitleId = internalTitleId;
        this.workExperience = workExperience;
        this.driverLicense = driverLicense;
        this.activedAt = activedAt;
        this.passport = passport;
        this.remark = remark;
        this.architectureId = architectureId;
        this.homepage = homepage;
        this.category = category;
    }

    public String getResumeUpdatedAt() {
        return resumeUpdatedAt;
    }

    public void setResumeUpdatedAt(String resumeUpdatedAt) {
        this.resumeUpdatedAt = resumeUpdatedAt;
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

    public String getIsAuthor() {
        return isAuthor;
    }

    public void setIsAuthor(String isAuthor) {
        this.isAuthor = isAuthor;
    }

    public String getHomepage() {
        return homepage;
    }

    public void setHomepage(String homepage) {
        this.homepage = homepage;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getArchitectureId() {
        return architectureId;
    }

    public void setArchitectureId(String architectureId) {
        this.architectureId = architectureId;
    }


    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getActivedAt() {
        return activedAt;
    }

    public void setActivedAt(String activedAt) {
        this.activedAt = activedAt;
    }

    public String getPassport() {
        return passport;
    }

    public void setPassport(String passport) {
        this.passport = passport;
    }

    public float getExpectSalaryMonth() {
        return expectSalaryMonth;
    }

    public void setExpectSalaryMonth(float expectSalaryMonth) {
        this.expectSalaryMonth = expectSalaryMonth;
    }

    public float getSalaryMonth() {
        return salaryMonth;
    }

    public void setSalaryMonth(float salaryMonth) {
        this.salaryMonth = salaryMonth;
    }

    public String getDriverLicense() {
        return driverLicense;
    }

    public void setDriverLicense(String driverLicense) {
        this.driverLicense = driverLicense;
    }

    public String getWorkExperience() {
        return workExperience;
    }

    public void setWorkExperience(String workExperience) {
        this.workExperience = workExperience;
    }

    public String getSchoolId() {
        return schoolId;
    }

    public void setSchoolId(String schoolId) {
        this.schoolId = schoolId;
    }

    public String getInternalTitleId() {
        return internalTitleId;
    }

    public void setInternalTitleId(String internalTitleId) {
        this.internalTitleId = internalTitleId;
    }

    public String getSection() {
        return section;
    }

    public void setSection(String section) {
        this.section = section;
    }

    public CompanyInfo getCompanyInfo() {
        return companyInfo;
    }

    public void setCompanyInfo(CompanyInfo companyInfo) {
        this.companyInfo = companyInfo;
    }

    public String getTitleId() {
        return titleId;
    }

    public void setTitleId(String titleId) {
        this.titleId = titleId;
    }

    public String getMyProject() {
        return myProject;
    }

    public void setMyProject(String myProject) {
        this.myProject = myProject;
    }

    public String getZip() {
        return zip;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

    public String getAddressDetail() {
        return addressDetail;
    }

    public void setAddressDetail(String addressDetail) {
        this.addressDetail = addressDetail;
    }

    public String getInternalTitleName() {
        return internalTitleName;
    }

    public void setInternalTitleName(String internalTitleName) {
        this.internalTitleName = internalTitleName;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getContactId() {
        return contactId;
    }

    public void setContactId(int contactId) {
        this.contactId = contactId;
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

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getBirth() {
        return birth;
    }

    public void setBirth(String birth) {
        this.birth = birth;
    }

    public String getNation() {
        return nation;
    }

    public void setNation(String nation) {
        this.nation = nation;
    }

    public String getMarital() {
        return marital;
    }

    public void setMarital(String marital) {
        this.marital = marital;
    }

    public String getIsFertility() {
        return isFertility;
    }

    public void setIsFertility(String isFertility) {
        this.isFertility = isFertility;
    }

    public String getIsHouse() {
        return isHouse;
    }

    public void setIsHouse(String isHouse) {
        this.isHouse = isHouse;
    }

    public String getLiveFamily() {
        return liveFamily;
    }

    public void setLiveFamily(String liveFamily) {
        this.liveFamily = liveFamily;
    }

    public int getAccountProvince() {
        return accountProvince;
    }

    public void setAccountProvince(int accountProvince) {
        this.accountProvince = accountProvince;
    }

    public int getAccount() {
        return account;
    }

    public void setAccount(int account) {
        this.account = account;
    }

    public int getNativePlaceProvince() {
        return nativePlaceProvince;
    }

    public void setNativePlaceProvince(int nativePlaceProvince) {
        this.nativePlaceProvince = nativePlaceProvince;
    }

    public int getNativePlace() {
        return nativePlace;
    }

    public void setNativePlace(int nativePlace) {
        this.nativePlace = nativePlace;
    }

    public int getAddressProvince() {
        return addressProvince;
    }

    public void setAddressProvince(int addressProvince) {
        this.addressProvince = addressProvince;
    }

    public int getAddress() {
        return address;
    }

    public void setAddress(int address) {
        this.address = address;
    }

    public int getCurrentStatus() {
        return currentStatus;
    }

    public void setCurrentStatus(int currentStatus) {
        this.currentStatus = currentStatus;
    }

    public int getSrc() {
        return src;
    }

    public void setSrc(int src) {
        this.src = src;
    }

    public String getSrcNo() {
        return srcNo;
    }

    public void setSrcNo(String srcNo) {
        this.srcNo = srcNo;
    }

    public String getExpectCityIds() {
        return expectCityIds;
    }

    public void setExpectCityIds(String expectCityIds) {
        this.expectCityIds = expectCityIds;
    }

    public String getExpectCityNames() {
        return expectCityNames;
    }

    public void setExpectCityNames(String expectCityNames) {
        this.expectCityNames = expectCityNames;
    }

    public double getExpectSalaryFrom() {
        return expectSalaryFrom;
    }

    public void setExpectSalaryFrom(double expectSalaryFrom) {
        this.expectSalaryFrom = expectSalaryFrom;
    }

    public double getExpectSalaryTo() {
        return expectSalaryTo;
    }

    public void setExpectSalaryTo(double expectSalaryTo) {
        this.expectSalaryTo = expectSalaryTo;
    }

    public double getExpectAnnualSalaryFrom() {
        return expectAnnualSalaryFrom;
    }

    public void setExpectAnnualSalaryFrom(double expectAnnualSalaryFrom) {
        this.expectAnnualSalaryFrom = expectAnnualSalaryFrom;
    }

    public double getExpectAnnualSalaryTo() {
        return expectAnnualSalaryTo;
    }

    public void setExpectAnnualSalaryTo(double expectAnnualSalaryTo) {
        this.expectAnnualSalaryTo = expectAnnualSalaryTo;
    }

    public double getExpectAnnualSalary() {
        return expectAnnualSalary;
    }

    public void setExpectAnnualSalary(double expectAnnualSalary) {
        this.expectAnnualSalary = expectAnnualSalary;
    }

    public double getExpectBonus() {
        return expectBonus;
    }

    public void setExpectBonus(double expectBonus) {
        this.expectBonus = expectBonus;
    }

    public String getExpectWorkAt() {
        return expectWorkAt;
    }

    public void setExpectWorkAt(String expectWorkAt) {
        this.expectWorkAt = expectWorkAt;
    }

    public String getExpectType() {
        return expectType;
    }

    public void setExpectType(String expectType) {
        this.expectType = expectType;
    }

    public String getExpectPositionName() {
        return expectPositionName;
    }

    public void setExpectPositionName(String expectPositionName) {
        this.expectPositionName = expectPositionName;
    }

    public String getExpectIndustryName() {
        return expectIndustryName;
    }

    public void setExpectIndustryName(String expectIndustryName) {
        this.expectIndustryName = expectIndustryName;
    }

    public String getExpectRemarks() {
        return expectRemarks;
    }

    public void setExpectRemarks(String expectRemarks) {
        this.expectRemarks = expectRemarks;
    }

    public String getNotExpectCorporationName() {
        return notExpectCorporationName;
    }

    public void setNotExpectCorporationName(String notExpectCorporationName) {
        this.notExpectCorporationName = notExpectCorporationName;
    }

    public String getNotExpectCorporationIds() {
        return notExpectCorporationIds;
    }

    public void setNotExpectCorporationIds(String notExpectCorporationIds) {
        this.notExpectCorporationIds = notExpectCorporationIds;
    }

    public int getNotExpectCorporationStatus() {
        return notExpectCorporationStatus;
    }

    public void setNotExpectCorporationStatus(int notExpectCorporationStatus) {
        this.notExpectCorporationStatus = notExpectCorporationStatus;
    }

    public String getIsActive() {
        return isActive;
    }

    public void setIsActive(String isActive) {
        this.isActive = isActive;
    }

    public String getBonusText() {
        return bonusText;
    }

    public void setBonusText(String bonusText) {
        this.bonusText = bonusText;
    }

    public String getOptions() {
        return options;
    }

    public void setOptions(String options) {
        this.options = options;
    }

    public String getOtherWelfare() {
        return otherWelfare;
    }

    public void setOtherWelfare(String otherWelfare) {
        this.otherWelfare = otherWelfare;
    }

    public String getInterests() {
        return interests;
    }

    public void setInterests(String interests) {
        this.interests = interests;
    }

    public String getOverseas() {
        return overseas;
    }

    public void setOverseas(String overseas) {
        this.overseas = overseas;
    }

    public String getPoliticalStatus() {
        return politicalStatus;
    }

    public void setPoliticalStatus(String politicalStatus) {
        this.politicalStatus = politicalStatus;
    }

    public String getProjectInfo() {
        return projectInfo;
    }

    public void setProjectInfo(String projectInfo) {
        this.projectInfo = projectInfo;
    }

    public String getOtherInfo() {
        return otherInfo;
    }

    public void setOtherInfo(String otherInfo) {
        this.otherInfo = otherInfo;
    }

    public String getCard() {
        return card;
    }

    public void setCard(String card) {
        this.card = card;
    }

    public String getSpeciality() {
        return speciality;
    }

    public void setSpeciality(String speciality) {
        this.speciality = speciality;
    }

    public String getPhoneId() {
        return phoneId;
    }

    public void setPhoneId(String phoneId) {
        this.phoneId = phoneId;
    }

    public String getMailId() {
        return mailId;
    }

    public void setMailId(String mailId) {
        this.mailId = mailId;
    }

    public int getNationId() {
        return nationId;
    }

    public void setNationId(int nationId) {
        this.nationId = nationId;
    }

    public String getPhoto() {
        return photo;
    }

    public void setPhoto(String photo) {
        this.photo = photo;
    }

    public String getSelfRemark() {
        return selfRemark;
    }

    public void setSelfRemark(String selfRemark) {
        this.selfRemark = selfRemark;
    }

    public List<String> getFocusedCorporations() {
        return focusedCorporations;
    }

    public void setFocusedCorporations(List<String> focusedCorporations) {
        this.focusedCorporations = focusedCorporations;
    }

    public List<String> getFocusedFeelings() {
        return focusedFeelings;
    }

    public void setFocusedFeelings(List<String> focusedFeelings) {
        this.focusedFeelings = focusedFeelings;
    }

    public String getIsCore() {
        return isCore;
    }

    public void setIsCore(String isCore) {
        this.isCore = isCore;
    }

    public String getCustomization() {
        return customization;
    }

    public void setCustomization(String customization) {
        this.customization = customization;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getIsAddV() {
        return isAddV;
    }

    public void setIsAddV(String isAddV) {
        this.isAddV = isAddV;
    }

    public int getLockedTime() {
        return lockedTime;
    }

    public void setLockedTime(int lockedTime) {
        this.lockedTime = lockedTime;
    }

    public int getVerificationAt() {
        return verificationAt;
    }

    public void setVerificationAt(int verificationAt) {
        this.verificationAt = verificationAt;
    }

    public String getPractice() {
        return practice;
    }

    public void setPractice(String practice) {
        this.practice = practice;
    }

    public String getStudy() {
        return study;
    }

    public void setStudy(String study) {
        this.study = study;
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

    public String getIsOversea() {
        return isOversea;
    }

    public void setIsOversea(String isOversea) {
        this.isOversea = isOversea;
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

    public String getSchoolName() {
        return schoolName;
    }

    public void setSchoolName(String schoolName) {
        this.schoolName = schoolName;
    }

    public String getDisciplineName() {
        return disciplineName;
    }

    public void setDisciplineName(String disciplineName) {
        this.disciplineName = disciplineName;
    }

    public int getDegree() {
        return degree;
    }

    public void setDegree(int degree) {
        this.degree = degree;
    }

    public String getIsEntrance() {
        return isEntrance;
    }

    public void setIsEntrance(String isEntrance) {
        this.isEntrance = isEntrance;
    }

    public String getDisciplineDesc() {
        return disciplineDesc;
    }

    public void setDisciplineDesc(String disciplineDesc) {
        this.disciplineDesc = disciplineDesc;
    }
}
