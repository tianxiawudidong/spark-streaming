package com.ifchange.sparkstreaming.v1.mysql;

import org.apache.log4j.Logger;

import java.sql.SQLException;

/**
 * Created by Administrator on 2017/7/19.
 */
public class GetCompressById {

    private static Logger logger=Logger.getLogger(GetCompressById.class);

    public static void main(String[] args){
        long prefix=0;
        long baseNumber=0L;
        long resumeId=Long.parseLong(args[0]);
        if(resumeId>40000000){
            if(resumeId> 80000000){
                baseNumber=16;
            }else{
                baseNumber=8;
            }
        }else{
            baseNumber = 0;
        }
        prefix=resumeId%8+baseNumber;
        String host="";
        if(prefix %2 ==0){
            host="192.168.8.130";
        }else{
            host="192.168.8.132";
        }
        String dbName="icdc_"+prefix;
        try {
            Mysql mysql = new Mysql("maxwell", "QbnMFw4dUqZQH3ST", dbName, host);
            String sql="select compress from `"+dbName+"`.`resumes_extras` where id="+resumeId;
            logger.info(sql);
            String compress = mysql.executeQueryCompress(sql);
            logger.info(compress);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


}
