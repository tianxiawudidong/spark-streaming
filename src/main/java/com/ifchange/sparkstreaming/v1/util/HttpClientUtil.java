package com.ifchange.sparkstreaming.v1.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

/**
 * httpclient 工具类
 *
 * @author xudongjun
 */
public class HttpClientUtil {

    private static final Logger logger = Logger.getLogger(HttpClientUtil.class);

    /*
     * get请求
     * @param  url 地址
     * @return JSONObject
     */
    public static JSONObject httpGet(String url) throws Exception {
        //get请求返回结果
        JSONObject jsonResult = null;
        HttpClientBuilder hb = HttpClientBuilder.create();
        CloseableHttpClient client = hb.build();
        //发送get请求
        HttpGet request = new HttpGet(url);
        RequestConfig config = RequestConfig.custom().setCookieSpec(CookieSpecs.IGNORE_COOKIES).build();
        request.setConfig(config);
        HttpResponse response = client.execute(request);
//        请求发送成功，并得到响应
        if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
//            读取服务器返回过来的json字符串数据
            String strResult = EntityUtils.toString(response.getEntity());
//                System.out.println(strResult);
//            把json字符串转换成json对象
            jsonResult = JSONObject.parseObject(strResult);
//            url = URLDecoder.decode(url, "UTF-8");
        } else {
            logger.error("get请求提交失败:" + url);
        }
        client.close();
        return jsonResult;
    }

    /*
     * post请求
     * @param url            url地址
     * @param jsonParam      参数
     * @param noNeedResponse 不需要返回结果
     * @return JSONObject
     */
    public static JSONObject httpPost(String url, String jsonParam,String token) throws Exception {
        //post请求返回结果
        HttpClientBuilder hb = HttpClientBuilder.create();
        CloseableHttpClient client = hb.build();
        JSONObject jsonResult = null;
        HttpPost method = new HttpPost(url);
//        if(StringUtils.isBlank(token)){
//            throw new Exception("token is null");
//        }
        if(StringUtils.isBlank(jsonParam)){
            throw new Exception("jsonParam is null");
        }
        //解决中文乱码问题
        StringEntity entity = new StringEntity(jsonParam, "utf-8");
        entity.setContentEncoding("UTF-8");
        entity.setContentType("application/json");
        method.setEntity(entity);
        method.setHeader(new BasicHeader("token",token));
        HttpResponse result = client.execute(method);
        //请求发送成功，并得到响应
        if (result.getStatusLine().getStatusCode() == 200) {
            // 读取服务器返回过来的json字符串数据
            String str = EntityUtils.toString(result.getEntity(),"UTF-8");
            logger.info(str);
            // 把json字符串转换成json对象*
        }
        return jsonResult;
    }

    public static void main(String[] args) throws Exception {
//        String url = "http://api.map.baidu.com/geocoder/v2/?address=%E4%B8%8A%E6%B5%B7%E5%B8%82%E6%B5%A6%E4%B8%9C%E6%96%B0%E5%8C%BA%E4%B8%96%E7%BA%AA%E5%A4%A7%E9%81%9368%E5%8F%B7&output=json&ak=bAF9v1ZluIZzI1mT4BmYkPlKGG7ZUN67";
//        JSONObject json = HttpClientUtil.httpGet(url);
//        JSONObject json2 = HttpClientUtil.httpPost(url,null,false);

//        String parmas="<xml>" +
//                "<appid>wx9931744dbeabc02a</appid>" +
//                "<mch_id>1482116682</mch_id>" +
//                "<device_info>WEB</device_info>" +
//                "<time_start>1506648373</time_start>" +
//                "<nonce_str>8338512017-09-28 19:28:38</nonce_str>" +
//                "<body><![CDATA[]]></body>" +
//                "<out_trade_no>17092819282901</out_trade_no>" +
//                "<total_fee>100</total_fee>" +
//                "<spbill_create_ip>127.0.0.1</spbill_create_ip>" +
//                "<trade_type>JSAPI</trade_type>" +
//                "<openid>oGspI1uIhdt5uZTcYsFBf369UuhU</openid>" +
//                "<sign>3E55FE54CF632653F4B5B54A2B1869A2</sign>" +
//                "<attach>test</attach></xml>";
//        String url="https://api.mch.weixin.qq.com/pay/unifiedorder";
//
//        String post = HttpsUtil.post(url, parmas);
//        System.out.print(post);
        JSONObject param = new JSONObject();
        param.put("resume_version_id","abc");
        param.put("resume_data","test1111");
        String url="http://localhost:8080/icdc/resume/save";
        String token="";
        HttpClientUtil.httpPost(url,param.toJSONString(),token);

    }

}
