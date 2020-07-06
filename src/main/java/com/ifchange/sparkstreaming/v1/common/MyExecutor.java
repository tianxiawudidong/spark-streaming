/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ifchange.sparkstreaming.v1.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author root
 */
public class MyExecutor {

    public static ExecutorService newFixedThreadPool(int nThreads, int queue_size) {
        return new ThreadPoolExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(queue_size),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static ExecutorService newFixedThreadPool(int nThreads) {
        return new ThreadPoolExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(512),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static ExecutorService newSingleThreadExecutor(int queue_size) {
        return new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(queue_size),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static ExecutorService newSingleThreadExecutor() {
        return new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(50),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }
}
