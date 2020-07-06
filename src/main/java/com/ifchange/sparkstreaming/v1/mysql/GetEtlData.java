package com.ifchange.sparkstreaming.v1.mysql;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017/7/19.
 */
public class GetEtlData {

    private static Logger logger=Logger.getLogger(GetEtlData.class);

    private static SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @SuppressWarnings("unchecked")
    public static void main(String[] args){
        long baseNumber;
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
        long prefix=resumeId%8+baseNumber;
        String host;
        if(prefix %2 ==0){
            host="192.168.8.130";
        }else{
            host="192.168.8.132";
        }
        String dbName="icdc_"+prefix;
        try {
            Mysql mysql = new Mysql("maxwell", "QbnMFw4dUqZQH3ST", dbName, host);
            String sql = "select e.`compress`,e.`id`,r.`work_experience`,r.`resume_updated_at`,column_json(a.data) as `arth` "
                    + " from " + dbName + ".`resumes` r "
                    + " left join " + dbName + ".`resumes_extras` e on r.id=e.id "
                    + " left join " + dbName + ".`algorithms` a on r.id=a.id"
                    + " where r.id=" + resumeId;
            logger.info("sql========[" + sql + "]");
            //简历原始数据+算法数据
            Map<String, Object> map = mysql.executeQueryResumeAndArth2(sql);
            Map<String, String> result = new HashMap();
            result.put("resume_id", String.valueOf(resumeId));
            result.put("resume_data",String.valueOf(map.get("resume_data")));
            if(null != map.get("resume_updated_at")){
                Timestamp resume_updated_at =(Timestamp)map.get("resume_updated_at");
                result.put("resume_updated_at",sdf.format(resume_updated_at) );
            }else{
                result.put("resume_updated_at","0");
            }
            result.put("work_experience", String.valueOf(map.get("work_experience")));
            String arth = String.valueOf(map.get("arth"));
            if (StringUtils.isNotBlank(arth)) {
                JSONObject json = JSONObject.parseObject(arth);
                if (null != json) {
                    //Cvtrade
                    String cv_trade = json.getString("cv_trade");
                    result.put("cv_trade", cv_trade);
                    //Cvtag
                    String cv_tag = json.getString("cv_tag");
                    result.put("cv_tag", cv_tag);
                    //Cvtitle
                    String cvTitile = json.getString("cv_title");
                    result.put("cv_title", cvTitile);
                    //Cvfeature
                    String cv_feature = json.getString("cv_feature");
                    result.put("cv_feature", cv_feature);
                    //Cveducation
                    String cv_education = json.getString("cv_education");
                    result.put("cv_education", cv_education);
                    String cv_degree = json.getString("cv_degree");
                    result.put("cv_degree", cv_degree);
                    //Cventity
                    String cv_entity = json.getString("cv_entity");
                    result.put("cv_entity", cv_entity);
                }
            }
            String str = JSON.toJSONString(result);
            logger.info(str);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


}
