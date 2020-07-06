package com.ifchange.sparkstreaming.v1.gm;


import com.ifchange.sparkstreaming.v1.common.MyDate;
import com.ifchange.sparkstreaming.v1.selib.gearman.SLGearmanClient;
import org.msgpack.MessagePack;
import org.msgpack.template.Templates;
import org.msgpack.type.Value;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author root
 */
public abstract class GearmanParam implements GearmanResult {

    protected Map<String, String> headerMap = new LinkedHashMap<>();
    protected Map<String, Object> msgMap = new LinkedHashMap<>();
    protected Map<Object, Object> requestMap = new LinkedHashMap<>();
    protected MessagePack MsgPack = new MessagePack();
    private static final String ConfFileName = "gm.conf";
    protected GearmanPool gearmanPool = null;
    protected byte[] gmResult = null;
    protected String resultJson = "";
    //汇总结果
    protected String totalResultJson = "";
    protected byte[] sendMsg = null;
    protected String reusmeId;
    protected String workName;
    protected String fieldName;
    protected String positionId;

    protected GearmanParam(GearmanPool gm_pool, String work_name, String field_name) {
        headerMap.put("session_id", "");
        headerMap.put("log_id", "");
        headerMap.put("product_name", "resumeFlashCvrade");
        headerMap.put("local_ip", "user100");
        headerMap.put("uid", "");
        try {
            headerMap.put("user_ip", InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException ex) {
            ex.printStackTrace();
        }
        msgMap.put("header", headerMap);
        msgMap.put("request", requestMap);
        gearmanPool = gm_pool;
        workName = work_name;
        fieldName = field_name;
    }
    
    
    protected GearmanParam(String work_name, String field_name) {
        headerMap.put("session_id", "");
        headerMap.put("log_id", "");
        headerMap.put("product_name", "resumeFlashCvrade");
        headerMap.put("local_ip", "user100");
        headerMap.put("uid", "");
        try {
            headerMap.put("user_ip", InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException ex) {
            ex.printStackTrace();
        }
        msgMap.put("header", headerMap);
        msgMap.put("request", requestMap);
        workName = work_name;
        fieldName = field_name;
    }
    

    public Map<String, Object> getMsgMap() {
        return msgMap;
    }

    protected void putRequest(Object key, Object ob) {
        requestMap.put(key, ob);
    }

    protected byte[] packMsg(Map<String, Object> msg) throws IOException {
        return MsgPack.write(msg);
    }

    protected Map<String, Value> unPackMsg(byte[] msg_bytes) throws IOException {
        Map<String, Value> result_map = MsgPack.read(msg_bytes, Templates.tMap(Templates.TString, Templates.TValue));
        return result_map;
    }

    public static String getConfigFileName() {
        return ConfFileName;
    }

    protected void putQueuePool(String result_json) {
        //添加写入队列
        Map<String, String> jsonMap = new HashMap<>();
        jsonMap.put("resume_id", reusmeId);
        jsonMap.put("col", fieldName);
        jsonMap.put("json", result_json);
    }
    protected void putQueuePool(String result_json, String fieldname) {
        //添加写入队列
        Map<String, String> jsonMap = new HashMap<>();
        jsonMap.put("resume_id", reusmeId);
        jsonMap.put("col", fieldname);
        jsonMap.put("json", result_json);
    }
    protected boolean submit(int time_out) throws Exception {
    	byte[] resultBytes = null;
        //请求gearman
        if (!packSendMsg()) {
            return false;
        }
        SLGearmanClient gmaseClient = gearmanPool.getGmConn();
        long start_time = System.currentTimeMillis();
        resultBytes = gmaseClient.sync_call(sendMsg, time_out);
        long user_time = System.currentTimeMillis() - start_time;
//        InitBrushServer.TimeCountThreads.execute(new ExecuteTimeCount(workName, user_time));
        gearmanPool.free(gmaseClient);//释放连接
        if (resultBytes == null || resultBytes.length == 0) {
            gmaseClient.reboot();
            return false;
        }
        gmResult = resultBytes;
        if (!parseResult()) {
            return false;
        }
        return true;
    }

    protected boolean submit() {
        boolean result = false;
        int count = 0;
        while (true) {
            count++;
            try {
                result = submit(1000);
                if ((result && !resultJson.equals("empty")) || count > 3) {
                    break;
                }
                String showmsg = "***********[%s][%s]resume_id:%s Gearman request failed, retry %d time";
                System.out.println(String.format(showmsg, Thread.currentThread().getName(), workName, reusmeId, count));
                //Thread.sleep((long) (Math.random() * 1000));
            } catch (Exception ex) {
                String msg = "%s    %s  work:%s resume_id:%s Insert msg:%s";
                System.out.println(String.format(msg, MyDate.stampToDate(System.currentTimeMillis()),Thread.currentThread().getName(), workName, reusmeId, ex.getMessage()));
                break;
            }
        }
        if (resultJson.equals("empty") || resultJson.isEmpty()) {
            resultJson = "";
            String showmsg = "[%s]resume_id:%s";
//            InitBrushServer.workerNullLogger.info(String.format(showmsg, workName, reusmeId));
        }
        if (!result) {
            String showmsg = "[%s]resume_id:%s";
//            InitBrushServer.workerErrorLogger.info(String.format(showmsg, workName, reusmeId));
        }
        return result;
    }
    
}
