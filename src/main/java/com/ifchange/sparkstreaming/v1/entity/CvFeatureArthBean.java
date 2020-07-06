package com.ifchange.sparkstreaming.v1.entity;

import org.msgpack.annotation.Message;

import java.io.Serializable;
import java.util.List;

@Message
public class CvFeatureArthBean implements Serializable {

    private List<CvFeatureArthWork> work;

    private List<CvFeatureArthProject> project;

    public List<CvFeatureArthWork> getWork() {
        return work;
    }

    public void setWork(List<CvFeatureArthWork> work) {
        this.work = work;
    }

    public List<CvFeatureArthProject> getProject() {
        return project;
    }

    public void setProject(
        List<CvFeatureArthProject> project) {
        this.project = project;
    }

    public CvFeatureArthBean(){}

    public CvFeatureArthBean(
        List<CvFeatureArthWork> work,
        List<CvFeatureArthProject> project) {
        this.work = work;
        this.project = project;
    }



}
