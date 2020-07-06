package com.ifchange.sparkstreaming.v1.util;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TimeUtil {
	
	
	private static final Logger logger = Logger.getLogger(TimeUtil.class);
	
	/**
	 *  根据开始时间 结束时间 得到这段工作的工作时长(单位：月)
	 * @param startTime  xxxx年xx月  xxxx-xx
	 * @param endTime    xxxx年xx月
	 * @return
	 */
	public static int getWorkTime(String startTime,String endTime){
//		logger.info("startTime:"+startTime+"\t"+"endTime:"+endTime);
		SimpleDateFormat sdfs = new SimpleDateFormat("yyyy年MM月");
		String regex="^\\d{4}年(\\d{1}|\\d{2})月$";
		String regex2="^\\d{4}-(\\d{1}|\\d{2})$";
		int startYear=0;
		int endYear=0;
		int startMonth=0;
		int endMonth=0;
		Pattern pattern = Pattern.compile(regex);
		Pattern pattern2 = Pattern.compile(regex2);
		int workTime=0;
		if(StringUtils.isBlank(startTime)){
			logger.error("工作开始时间为空,计算不出工作时长");
		}else{
			if(StringUtils.isNotBlank(endTime)){
				Matcher matcher = pattern.matcher(startTime);
				if(matcher.matches()){
					startYear=Integer.parseInt(startTime.split("年")[0]);
					startMonth=Integer.parseInt(startTime.split("月")[0].split("年")[1]);
				}else{
					Matcher matcher2 = pattern2.matcher(startTime);
					if(matcher2.matches()){
						startYear=Integer.parseInt(startTime.split("-")[0]);
						startMonth=Integer.parseInt(startTime.split("-")[1]);
					}
				}
				Matcher matcher_end = pattern.matcher(endTime);
				if(matcher_end.matches()){
					endYear=Integer.parseInt(endTime.split("年")[0]);
					endMonth=Integer.parseInt(endTime.split("月")[0].split("年")[1]);
				}else{
					Matcher matcher2 = pattern2.matcher(endTime);
					if(matcher2.matches()){
						endYear=Integer.parseInt(endTime.split("-")[0]);
						endMonth=Integer.parseInt(endTime.split("-")[1]);
					}
				}
				
				if(startYear>endYear || ((startYear==endYear) && (startMonth>endMonth))){
					logger.error("开始时间:["+startTime+"]大于结束时间:["+endTime+"],不合法");
					workTime=-1;
				}else{
					int diffYear=endYear-startYear;
					if(endMonth<=startMonth){
						workTime=(endMonth+12-startMonth)+12*(diffYear-1);
					}else{
						workTime=(endMonth-startMonth)+12*diffYear;
					}
				}
			}else{//没有结束时间---工作至今 结束时间xxxx年xx月
				Date date = new Date();
				endTime=sdfs.format(date);
				logger.info("结束时间:"+endTime);
				Matcher matcher = pattern.matcher(startTime);
				if(matcher.matches()){
					startYear=Integer.parseInt(startTime.split("年")[0]);
					startMonth=Integer.parseInt(startTime.split("月")[0].split("年")[1]);
				}else{
					Matcher matcher2 = pattern2.matcher(startTime);
					if(matcher2.matches()){
						startYear=Integer.parseInt(startTime.split("-")[0]);
						startMonth=Integer.parseInt(startTime.split("-")[1]);
					}
				}
				endYear=Integer.parseInt(endTime.split("年")[0]);
				endMonth=Integer.parseInt(endTime.split("月")[0].split("年")[1]);
				if(startYear>endYear || ((startYear==endYear) && (startMonth>endMonth))){
					logger.error("开始时间:["+startTime+"]大于结束时间:["+endTime+"],不合法");
					workTime=-1;
				}else{
					int diffYear=endYear-startYear;
					if(endMonth<=startMonth){
						workTime=(endMonth+12-startMonth)+12*(diffYear-1);
					}else{
						workTime=(endMonth-startMonth)+12*diffYear;
					}
				}
			}
		}
		return workTime;
	}
	
	/**
	 * 比较两段时间 ，取最小的时间
	 * @param startTime1  xxxx年xx月
	 * @param startTime2  xxxx年xx月
	 * @return
	 */
	public static String getMinDate(String startTime1,String startTime2){
		if(StringUtils.isNotBlank(startTime1) && StringUtils.isBlank(startTime2)){
			return startTime1;
		}else if(StringUtils.isNotBlank(startTime2) && StringUtils.isBlank(startTime1)){
			return startTime2;
		}else{
			if(startTime1.contains("年")){
				//年份
				int year1=Integer.parseInt(startTime1.split("年")[0]);
				int year2=Integer.parseInt(startTime2.split("年")[0]);
				//月份
				int month1=Integer.parseInt(startTime1.split("年")[1].split("月")[0]);
				int month2=Integer.parseInt(startTime2.split("年")[1].split("月")[0]);
				if(year1>year2){
					return startTime2;
				}else if(year1<year2){
					return startTime1;
				}else{//年份相同 比较月份
					if(month1>month2){
						return startTime2;
					}else if(month1<month2){
						return startTime1;
					}else{ //年 月 相同 随便返回一个
						return startTime1;
					}
				}
			}else if(startTime1.contains("-")){
				//年份
				int year1=Integer.parseInt(startTime1.split("-")[0]);
				int year2=Integer.parseInt(startTime2.split("-")[0]);
				//月份
				int month1=Integer.parseInt(startTime1.split("-")[1]);
				int month2=Integer.parseInt(startTime2.split("-")[1]);
				if(year1>year2){
					return startTime2;
				}else if(year1<year2){
					return startTime1;
				}else{//年份相同 比较月份
					if(month1>month2){
						return startTime2;
					}else if(month1<month2){
						return startTime1;
					}else{ //年 月 相同 随便返回一个
						return startTime1;
					}
				}
			}else{
				//年份
				int year1=Integer.parseInt(startTime1.substring(0,4));
				int year2=Integer.parseInt(startTime2.substring(0,4));
				//月份
				int month1=0;
				try {
					month1=Integer.parseInt(startTime1.substring(5,7));
				} catch (NumberFormatException e) {
					month1=Integer.parseInt(startTime1.substring(5,6));
				}
				int month2=0;
				try {
					month2=Integer.parseInt(startTime2.substring(5,7));
				} catch (NumberFormatException e) {
					month2=Integer.parseInt(startTime2.substring(5,6));
				}
				if(year1>year2){
					return startTime2;
				}else if(year1<year2){
					return startTime1;
				}else{//年份相同 比较月份
					if(month1>month2){
						return startTime2;
					}else if(month1<month2){
						return startTime1;
					}else{ //年 月 相同 随便返回一个
						return startTime1;
					}
				}
			}

		}
	}
	
	/**
	 * 比较两段时间 ，取最大的时间
	 * @param startTime1  xxxx年xx月
	 * @param startTime2  xxxx年xx月
	 * @return
	 */
	public static String getMaxDate(String startTime1,String startTime2){
		if(StringUtils.isNotBlank(startTime1) && StringUtils.isBlank(startTime2)){
			return startTime2;
		}else if(StringUtils.isNotBlank(startTime2) && StringUtils.isBlank(startTime1)){
			return startTime1;
		}else if(StringUtils.isBlank(startTime1) && StringUtils.isBlank(startTime2)){
			return startTime1;
		} else{
			if(startTime1.contains("年")){
				//年份
				int year1=Integer.parseInt(startTime1.split("年")[0]);
				int year2=Integer.parseInt(startTime2.split("年")[0]);
				//月份
				int month1=Integer.parseInt(startTime1.split("年")[1].split("月")[0]);
				int month2=Integer.parseInt(startTime2.split("年")[1].split("月")[0]);
				if(year1>year2){
					return startTime1;
				}else if(year1<year2){
					return startTime2;
				}else{//年份相同 比较月份
					if(month1>month2){
						return startTime1;
					}else if(month1<month2){
						return startTime2;
					}else{ //年 月 相同 随便返回一个
						return startTime1;
					}
				}
			} else if(startTime1.contains("-")){
				//年份
				int year1=Integer.parseInt(startTime1.split("-")[0]);
				int year2=Integer.parseInt(startTime2.split("-")[0]);
				//月份
				int month1=Integer.parseInt(startTime1.split("-")[1]);
				int month2=Integer.parseInt(startTime2.split("-")[1]);
				if(year1>year2){
					return startTime1;
				}else if(year1<year2){
					return startTime2;
				}else{//年份相同 比较月份
					if(month1>month2){
						return startTime1;
					}else if(month1<month2){
						return startTime2;
					}else{ //年 月 相同 随便返回一个
						return startTime1;
					}
				}
			}else{
				//年份
				int year1=Integer.parseInt(startTime1.substring(0,4));
				int year2=Integer.parseInt(startTime2.substring(0,4));
				//月份
				int month1=0;
				try {
					month1=Integer.parseInt(startTime1.substring(5,7));
				} catch (NumberFormatException e) {
					month1=Integer.parseInt(startTime1.substring(5,6));
				}
				int month2=0;
				try {
					month2=Integer.parseInt(startTime2.substring(5,7));
				} catch (NumberFormatException e) {
					month2=Integer.parseInt(startTime2.substring(5,6));
				}
				if(year1>year2){
					return startTime1;
				}else if(year1<year2){
					return startTime2;
				}else{//年份相同 比较月份
					if(month1>month2){
						return startTime1;
					}else if(month1<month2){
						return startTime2;
					}else{ //年 月 相同 随便返回一个
						return startTime1;
					}
				}
			}

		}
	}
	
	public static void main(String[] args){
		String maxDate = getMaxDate("", "12131");
		System.out.println(maxDate);
	}
	
}
