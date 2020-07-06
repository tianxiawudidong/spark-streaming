/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ifchange.sparkstreaming.v1.common;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author root
 */
public class ExecuteTimeCount implements Runnable {

    private String name = "default";
    private int time = 0;
    private int countQpsPerNumber = 50; //每50次请求计算一次qps
    public static Map<String, Map> ClassExeMap = new ConcurrentHashMap(100);

    public ExecuteTimeCount(String pname, Long ptime) {
        name = pname;
        time = ptime.intValue();
    }

    @Override
    public void run() {
        synchronized (name.intern()) {//行锁
            Map<String, Object> timeMap = new HashMap();
            timeMap = ClassExeMap.get(name);
            if (timeMap == null) {
                timeMap = new HashMap();
                timeMap.put("max", 0);
                timeMap.put("min", time);
                timeMap.put("avg", new Float(0));
                timeMap.put("count", 0);
                timeMap.put("starttime", System.currentTimeMillis());
            }
            int count = (int) timeMap.get("count");
            float avg = Float.valueOf(String.valueOf(timeMap.get("avg")));
            avg = (float) ((avg * count + time) / (count + 1));
            count++;
            if (count % 10000 == 0) {
                long start_time = (long) timeMap.get("starttime");
                long this_time = System.currentTimeMillis();
                int qps = Math.round(10000 * 1000 / (this_time - start_time));
                timeMap.put("qps", qps);
                timeMap.put("starttime", this_time);
            }
            timeMap.put("max", Math.max((int) (timeMap.get("max")), time));
            timeMap.put("min", Math.min((int) (timeMap.get("min")), time));
            timeMap.put("avg", avg);
            timeMap.put("count", count);
            ClassExeMap.put(name, timeMap);
        }
    }

}
