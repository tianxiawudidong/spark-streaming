package com.ifchange.sparkstreaming.v1.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 日志参数解析工具类
 * Created by Administrator on 2017/10/18.
 */
public class ParamParseUtil {

    /*
     * @param str t=2017-10-17 17:40:08&f=icdc_33506769493350686128&s=1&r=0.069
     * @return
     */
    public static Map<String,String> parse(String str) throws Exception{
        Map<String,String> result=new HashMap<>();
        String[] split = str.split("&");
        if(split.length>0){
            for(String text:split){
                String[] split1 = text.split("=");
                if(split1.length==2){
                    String key=split1[0];
                    String value=split1[1];
                    result.put(key,value);
                }
                if(split1.length==1){
                    String key=split1[0];
                    String value="";
                    result.put(key,value);
                }
            }
        }
        return result;
    }

    public static void main(String[] args){
        String str="INFO t=2018-01-02 17:20:55&f=jd_bi.py&[line:107]&&f=jd_bi&logid=123456&w=jd_bi&c=jd_bi&m=getAllPreferences&s=1&r=0.000507116317749&hostname=[192.168.8.67:4730]&memory=1.2M&corp_time=6.50882720947e-05&school_time=6.50882720947e-05";
        String str2=str.substring(str.indexOf("t="),str.length());
        System.out.println(str2);
        try {
            Map<String, String> parse = ParamParseUtil.parse(str2);
            Set<String> keys = parse.keySet();
            for (String key: keys) {
                System.out.println(key+"--->"+parse.get(key));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
