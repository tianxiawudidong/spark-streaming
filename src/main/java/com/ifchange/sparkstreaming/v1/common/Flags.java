package com.ifchange.sparkstreaming.v1.common;

/**
 * spark streaming Flag类
 * <p>
 * Created by Administrator on 2017/8/2.
 */
public class Flags {

    private static Flags THE_INSTANCE = new Flags();

    private boolean initialized = false;

    // 窗口大小 窗口计算的时间跨度
    private static final long windowLength=30;

    // 计算间隔 窗口计算的频度
    private static final long slideInterval=5;

    private Flags() {
    }

    public static Flags getInstance() {
        if (!THE_INSTANCE.initialized) {
            throw new RuntimeException("Flags have not been initialized");
        }
        return THE_INSTANCE;
    }

}
