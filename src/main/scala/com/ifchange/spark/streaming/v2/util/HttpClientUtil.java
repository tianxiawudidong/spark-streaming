package com.ifchange.spark.streaming.v2.util;


import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * httpclient 工具类
 *
 * @author xudongjun
 */
public class HttpClientUtil {

    private static final Logger logger = LoggerFactory.getLogger(HttpClientUtil.class);

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
    public static String httpPost(String url, String jsonParam) throws Exception {
        //post请求返回结果
        HttpClientBuilder hb = HttpClientBuilder.create();
        CloseableHttpClient client = hb.build();
//        JSONObject jsonResult = null;
        String str = "";
        HttpPost method = new HttpPost(url);
        if (StringUtils.isBlank(jsonParam)) {
            throw new Exception("jsonParam is null");
        }
        //解决中文乱码问题
        StringEntity entity = new StringEntity(jsonParam, "utf-8");
        entity.setContentEncoding("UTF-8");
        entity.setContentType("application/json");
        method.setEntity(entity);
//        method.setHeader(new BasicHeader("token",""));
        HttpResponse result = client.execute(method);
        //请求发送成功，并得到响应
        if (result.getStatusLine().getStatusCode() == 200) {
            // 读取服务器返回过来的json字符串数据
            str = EntityUtils.toString(result.getEntity(), "UTF-8");
            logger.info(str);
//            jsonResult = JSONObject.parseObject(str);
            // 把json字符串转换成json对象*
        }
        client.close();
        return str;
    }


}
