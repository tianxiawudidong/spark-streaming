package com.ifchange.sparkstreaming.v1.gm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ifchange.sparkstreaming.v1.common.MyDate;
import com.ifchange.sparkstreaming.v1.common.MyExecutor;
import com.ifchange.sparkstreaming.v1.selib.gearman.SLGearmanClient;
import org.msgpack.template.Templates;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Converter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author root
 */
public class PositionAddressGearman extends GearmanParam implements Callable<String> {

    private static String WorkName = "icdc_position_save";
    private static String FieldName = "icdc_position_address";
    private static GearmanPool gearmanPool = null;
    private static int ThreadNumber = 2;//执行该work的线程池的最大线程数
    private static ThreadPoolExecutor ThreadPool;

    public PositionAddressGearman(int positionId, Map<String, Object> resumeMap) {
        super(gearmanPool, WorkName, FieldName);
        super.positionId = String.valueOf(positionId);
        super.putRequest("c", "positions/logic_position");
        super.putRequest("m", "update_address_by_pid");
        super.putRequest("p", resumeMap);
        headerMap.put("log_id", String.format("%d_%d_%d", positionId, System.currentTimeMillis(), Thread.currentThread().getId()));
    }

    @Override
    public boolean packSendMsg() {
        try {
            sendMsg = packMsg(msgMap);
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean parseResult() {
        Map<String, Value> result_map;
        try {
            result_map = unPackMsg(gmResult);
            Value reponse_map = result_map.get("response");
            Map<String, Value> creponseMap = new Converter(reponse_map).read(Templates.tMap(Templates.TString, Templates.TValue));
            int status = new Converter(creponseMap.get("err_no")).read(Templates.TInteger);
            String msg = new Converter(creponseMap.get("err_msg")).read(Templates.TString);
            if (status != 0) {
                String showmsg = "***********[%s]resume_id:%s Gearman retrun error:%s, status:%d";
                System.out.println(String.format(showmsg, WorkName, reusmeId, msg, status));
                return false;
            }
            Value result = creponseMap.get("results");
            if (result == null) {
                resultJson = "empty";
                return true;
            }
            resultJson = JSON.toJSONString(JSON.parse(result.toString()), SerializerFeature.BrowserCompatible);
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    private static void setGearmanPool() throws Exception {
        gearmanPool = new GearmanPool(WorkName);
        gearmanPool.setMaxNumber(ThreadNumber * 2);
    }

    public static GearmanPool getGearmanPool() {
        return gearmanPool;
    }

    public static ThreadPoolExecutor getThreadPool() {
        return ThreadPool;
    }

    public static void init(int thead_number, String worker_name, String worker_field_name) throws Exception {
        if (worker_name != null) {
            WorkName = worker_name;
        }
        if (worker_name != null) {
            FieldName = worker_field_name;
        }
        if (thead_number > 0) {
            ThreadNumber = thead_number;
            ThreadPool = (ThreadPoolExecutor) MyExecutor.newFixedThreadPool(ThreadNumber);
            setGearmanPool();
        } else {
            init();
        }
    }

    private static void init() throws Exception {
        ThreadPool = (ThreadPoolExecutor) MyExecutor.newFixedThreadPool(ThreadNumber);
        setGearmanPool();
    }

    @Override
    public String call() {
        String result = "";
        boolean flag = submit();
        if (flag) {
            result = resultJson;
        }

        return result;
    }

    @Override
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
                System.out.println(String.format(msg, MyDate.stampToDate(System.currentTimeMillis()), Thread.currentThread().getName(), workName, reusmeId, ex.getMessage()));
                break;
            }
        }
        if (resultJson.equals("empty") || resultJson.isEmpty()) {
            resultJson = "";
        }
//        if (!result) {
//            String showmsg = "[%s]resume_id:%s";
//        }
        return result;
    }

    @Override
    protected boolean submit(int time_out) throws Exception {
        byte[] resultBytes;
        //请求gearman
        if (!packSendMsg()) {
            return false;
        }
        SLGearmanClient gmaseClient = gearmanPool.getGmConn();
        resultBytes = gmaseClient.sync_call(sendMsg, time_out);
        gearmanPool.free(gmaseClient);//释放连接
        if (resultBytes == null || resultBytes.length == 0) {
            gmaseClient.reboot();
            return false;
        }
        gmResult = resultBytes;
        return parseResult();
    }

//	public static void main(String[] args) {
//		Map<String, Object> resumeMap=new HashMap<String, Object>();
//		resumeMap.put("position_id", 2261928);
//		resumeMap.put("jd_address", "xyz");
//		resumeMap.put("flag_updated", 1);
//		try {
//			PositionAddressGearman.init(2,"icdc_position_save","icdc_position_address");
//			PositionAddressGearman positionAddress = new PositionAddressGearman(2261928, resumeMap);
//			Future<String> result = PositionAddressGearman.getThreadPool().submit(positionAddress);
//			System.out.println(result.get());
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
}
