package com.ifchange.sparkstreaming.v1.util;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.sparkstreaming.v1.entity.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class WorkUtil {

    private static final Logger logger = LoggerFactory.getLogger(WorkUtil.class);

    /*
     * 工作经历排序
     * 当前简历解析结果中工作经历顺序为源文本中填写顺序，可能存在时间上的乱序影响bi、展示等。
     * 需求在简历入库前对工作经历进行排序，按时间顺序由新到旧。
     * 排序规则：
     * 1、全部工作经历中，有任意一段无begin_time，不做重排；
     * 2、全部工作经历中，有任意一段无end_time且so_far不是Y，不做重排；
     * 3、全部工作经历按开始时间由新到旧排序；
     * 4、开始时间相同的，按原相对顺序排序；
     * 5、开始时间需要注意9月和09月，即1位数字和2位数字的比较；
     * 补充一个异常case，时间可能有的有月份 有的没有。
     * 比较策略统一按先比年份大小，后比月份大小，如果有缺失，按原相对顺序。
     * 即如果原顺序中是2014年和2014年9月两种开始时间比较，那么原来哪段在先的就保持在先
     *
     */
    public static void workSort(List<Work> workList) {

        for (Work oldWork : workList) {
            String startTime = oldWork.getStartTime();
            //全部工作经历中，有任意一段无begin_time，不做重排；
            if (StringUtils.isBlank(startTime)) {
                break;
            }
            //全部工作经历中，有任意一段无end_time且so_far不是Y，不做重排；
            String endTime = oldWork.getEndTime();
            String soFar = oldWork.getSoFar();
            if (StringUtils.isBlank(endTime) && !soFar.equals("Y")) {
                break;
            }
            //如果格式不满足 年 月  不做重排
            if (!startTime.contains("年") || startTime.length() < 5) {
                break;
            }
        }

        logger.info("排序前的数据为:---------------------------");
        for (Work oldWork : workList) {
            logger.info(oldWork.getId() + "------" + oldWork.getStartTime());
        }
        logger.info("----------------------------------------------");

        //开始排序 全部工作经历按开始时间由新到旧排序；
        Collections.sort(workList, (o1, o2) -> {
            int result;
            String startTime1 = o1.getStartTime();
            String startTime2 = o2.getStartTime();
            int year1 = Integer.parseInt(startTime1.substring(0, startTime1.indexOf("年")));
            int year2 = Integer.parseInt(startTime2.substring(0, startTime2.indexOf("年")));

            if (year1 > year2) {
                result = -1;
            } else if (year1 < year2) {
                result = 1;
            } else {
                if (startTime1.contains("月") && startTime2.contains("月")) {
                    int month1 = Integer.parseInt(startTime1.substring(startTime1.indexOf("年") + 1, startTime1.indexOf("月")));
                    int month2 = Integer.parseInt(startTime2.substring(startTime2.indexOf("年") + 1, startTime2.indexOf("月")));
                    result = Integer.compare(month2, month1);
                } else {
                    result = 0;
                }
            }
            return result;
        });

        logger.info("排序后的数据为:**********************************");
        for (Work oldWork : workList) {
            logger.info(oldWork.getId() + "------" + oldWork.getStartTime());
        }
        logger.info("*********************************************");
    }

    public static void workSortResumeWork(List<ResumeWork> workList) {

        for (ResumeWork oldWork : workList) {
            String startTime = oldWork.getStart_time();
            //全部工作经历中，有任意一段无begin_time，不做重排；
            if (StringUtils.isBlank(startTime)) {
                break;
            }
            //全部工作经历中，有任意一段无end_time且so_far不是Y，不做重排；
            String endTime = oldWork.getEnd_time();
            String soFar = oldWork.getSo_far();
            if (StringUtils.isBlank(endTime) && !soFar.equals("Y")) {
                break;
            }
            //如果格式不满足 年 月  不做重排
            if (!startTime.contains("年") || startTime.length() < 5) {
                break;
            }
        }

        logger.info("排序前的数据为:---------------------------");
        for (ResumeWork oldWork : workList) {
            logger.info(oldWork.getId() + "------" + oldWork.getStart_time());
        }
        logger.info("----------------------------------------------");

        //开始排序 全部工作经历按开始时间由新到旧排序；
        Collections.sort(workList, (ResumeWork o1, ResumeWork o2) -> {
            int result;
            String startTime1 = o1.getStart_time();
            String startTime2 = o2.getStart_time();
            int year1 = Integer.parseInt(startTime1.substring(0, startTime1.indexOf("年")));
            int year2 = Integer.parseInt(startTime2.substring(0, startTime2.indexOf("年")));

            if (year1 > year2) {
                result = -1;
            } else if (year1 < year2) {
                result = 1;
            } else {
                if (startTime1.contains("月") && startTime2.contains("月")) {
                    int month1 = Integer.parseInt(startTime1.substring(startTime1.indexOf("年") + 1, startTime1.indexOf("月")));
                    int month2 = Integer.parseInt(startTime2.substring(startTime2.indexOf("年") + 1, startTime2.indexOf("月")));
                    if (month1 > month2) {
                        result = -1;
                    } else if (month1 < month2) {
                        result = 1;
                    } else {
                        result = 0;
                    }
                } else {
                    result = 0;
                }
            }
            return result;
        });

        logger.info("排序后的数据为:**********************************");
        for (ResumeWork oldWork : workList) {
            logger.info(oldWork.getId() + "------" + oldWork.getStart_time());
        }
        logger.info("*********************************************");
    }


    /*
     * 工作状态  1 离职   0  非离职
     * "1"(离职，正在看机会)、"2"(在职，正在看机会)、"3"(在职，有好的机会可以考虑)、"4"(在职，不考虑机会)、"5"(实习生)'
     * 1、如果basic.current_status  为离职状态  或者
     * 2、最近一段工作经历有结束时间
     * 满足以下任一条 判断为已离职
     * 1、简历的最近一段工作经验不是在目标公司
     * 2、简历的最近一段工作经验是目标公司，且目前状态已离职
     * 3、简历的最近一段工作经验是目标公司，目前状态未完善，且在该公司的工作经验有具体的结束时间，如：2014-01 =  2016-12
     * <p>
     * 不满足以上任何一条件简历就判断为在职
     * <p>
     * 简历的最近一段工作经验是目标公司，目前状态不是已离职，且在该公司的工作经验没有具体的结束时间，即可判断当前在职，其他情况均为已离职。
     */
    @SuppressWarnings("unchecked")
    public static String getCvCurrentStatus(String compress) {
        String cvCurrentStatus = "0";
        JSONObject json = JSONObject.parseObject(compress);
        JSONObject basic = json.getJSONObject("basic");
        Map<String, Object> work = (Map<String, Object>) json.get("work");
        Map<String, Object> currentWorkMap = new HashMap<>();
        if (null != basic) {
            String currentStatus = basic.getString("current_status");
            if (StringUtils.isNotBlank(currentStatus) && currentStatus.equals("1")) {
                cvCurrentStatus = "1";
            }
        }
        if (null == work) {
            cvCurrentStatus = "2";
        } else {
            //取出最新的一份工作经历
            for (Map.Entry<String, Object> map : work.entrySet()) {
                Map<String, Object> workDetail = (Map<String, Object>) map.getValue();
                Object sort = workDetail.get("sort_id");
                if (null != sort) {
                    int sortId = (int) sort;
                    if (sortId == 1) {
                        currentWorkMap = workDetail;
                    }
                }
            }
            Object endTime = currentWorkMap.get("end_time");
            if (null != endTime) {
                cvCurrentStatus = "1";
            }

        }
        return cvCurrentStatus;
    }


    public static int getCvCurrentStatus(OriginResumeBean originResumeBean) {
        int cvCurrentStatus = 0;
        Basic basic = originResumeBean.getBasic();
        Map<String, Work> work = originResumeBean.getWork();
        Work currentWork = new Work();
        if (null != basic) {
            int currentStatus = basic.getCurrentStatus();
            if (currentStatus == 1) {
                cvCurrentStatus = 1;
            }
        }
        if (null == work) {
            cvCurrentStatus = 2;
        } else {
            //取出最新的一份工作经历
            for (Map.Entry<String, Work> map : work.entrySet()) {
                Work workDetail = map.getValue();
                int sortId = workDetail.getSortId();
                if (sortId == 1) {
                    currentWork = workDetail;
                }
            }
            String endTime = currentWork.getEndTime();
            if (StringUtils.isNotBlank(endTime)) {
                cvCurrentStatus = 1;
            }
        }
        return cvCurrentStatus;
    }

    public static Work getCurrentWork(OriginResumeBean originResumeBean) {
        Map<String, Work> workMap = originResumeBean.getWork();
        Work currentWork = new Work();
        if (null != workMap && workMap.size()>0) {
            for(Map.Entry<String,Work> entry:workMap.entrySet()){
                Work work = entry.getValue();
                if(work.getSortId()==1){
                    currentWork=work;
                }
            }
        }
        return currentWork;
    }


    public static void main(String[] args) {
        List<ResumeWork> workList = new ArrayList<>();
        ResumeWork work1 = new ResumeWork();
        work1.setId(3333333);
        work1.setStart_time("2013年");
        ResumeWork work2 = new ResumeWork();
        work2.setId(1111111);
        work2.setStart_time("2013年09月");
        ResumeWork work3 = new ResumeWork();
        work3.setId(4444444);
        work3.setStart_time("2014年11月");
        ResumeWork work4 = new ResumeWork();
        work4.setId(55555555);
        work4.setStart_time("2018年1月");
        ResumeWork work5 = new ResumeWork();
        work5.setId(666666666);
        work5.setStart_time("2018年11月");
        workList.add(work1);
        workList.add(work2);
        workList.add(work3);
        workList.add(work4);
        workList.add(work5);

        workSortResumeWork(workList);

    }
}
