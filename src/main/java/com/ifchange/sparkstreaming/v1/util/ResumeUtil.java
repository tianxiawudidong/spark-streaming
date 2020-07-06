package com.ifchange.sparkstreaming.v1.util;

public class ResumeUtil {

    private static final double RECORDNUM = 5000000;

    /**
     * 根据简历id计算出在哪个db
     *
     * @param resumeId 简历id
     * @return db
     */
    public static String getDBNameById(int resumeId) {
        String suffix = "icdc_";
//        if (!environment.equals("production")) {
        suffix = suffix + (int) Math.floor(resumeId / RECORDNUM);
//        } else {
//            suffix = suffix + (int) (resumeId % 8) + Math.floor(resumeId / 40000000) * 8;
//        }
        return suffix;
    }


    /**
     * 根据简历id计算出在哪个db
     *
     * @param resumeId 简历id
     * @return db
     */
    public static int getDB(int resumeId) {
        return (int) Math.floor(resumeId / RECORDNUM);
    }


    /**
     * 根据dbName选择相应的数据源
     *
     * @param dbName dbName
     * @return dataSource
     */
    public static String getDBType(String dbName) {
        //根据dbName取对应的source
        String[] split2 = dbName.split("_");
        int num = Integer.parseInt(split2[1]);
        return num % 2 == 0 ? "cvDataSource" : "cvDataSource2";
    }


//    public static void main(String[] args){
//        long resumeId=4999998;
//        String dbNameById = getDBNameById(resumeId);
//        System.out.println(dbNameById);
//    }

}
