/*
 * Copyright (C) 2009 by Eric Herman <eric@freesa.org>
 * Use and distribution licensed under the
 * GNU Lesser General Public License (LGPL) version 2.1.
 * See the COPYING file in the parent directory for full text.
 */
package com.ifchange.sparkstreaming.v1.selib.gearman;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.gearman.client.GearmanClient;
import org.gearman.client.GearmanClientImpl;
import org.gearman.client.GearmanJob;
import org.gearman.client.GearmanJobImpl;
import org.gearman.client.GearmanJobResult;
import org.gearman.common.GearmanNIOJobServerConnection;

public class MyGearmanClient {

    private GearmanClient client;
    private String name;
    private List<SLGearmanHost> hosts = new ArrayList<SLGearmanHost>();

    public MyGearmanClient(String cfile, String name) {
        this.name = name;
        client = new GearmanClientImpl();
        hosts = SLGearmanWorker.parseHosts(cfile, name);
        for (SLGearmanHost host : hosts) {
            GearmanNIOJobServerConnection conn = new GearmanNIOJobServerConnection(host.getIp(), host.getPort());
            client.addJobServer(conn);
        }
    }

    public MyGearmanClient(String host, String name, int port) {
        this.name = name;
        client = new GearmanClientImpl();
        GearmanNIOJobServerConnection conn = new GearmanNIOJobServerConnection(host, port);
        client.addJobServer(conn);
    }

    public GearmanJob asyncSubmit(byte[] data, String uniqueId) {
        GearmanJob job = GearmanJobImpl.createJob(name, data, uniqueId);
        client.submit(job);
        return job;
    }

    public GearmanJob asyncSubmit(byte[] data) {
        String uniqueId = null;
        return asyncSubmit(data, uniqueId);
    }

    public byte[] submit(byte[] data, String uniqueId, int time_out) {
        GearmanJob job = GearmanJobImpl.createJob(name, data, uniqueId);
        long start_time = System.currentTimeMillis();
        client.submit(job);
        long user_time = System.currentTimeMillis() - start_time;
//        InitBrushServer.TimeCountThreads.execute(new ExecuteTimeCount("submit", user_time));
        byte[] returnBytes = null;
        GearmanJobResult jobResult = null;
        try {
            start_time = System.currentTimeMillis();
            if (time_out <= 0) {
                jobResult = job.get();
            } else {
                jobResult = job.get(time_out, TimeUnit.MILLISECONDS);
            }
            user_time = System.currentTimeMillis() - start_time;
//            InitBrushServer.TimeCountThreads.execute(new ExecuteTimeCount("get", user_time));
            returnBytes = jobResult.getResults();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return returnBytes;
    }

    public byte[] submit(byte[] data, int time_out) {
        String uniqueId = null;
        return submit(data, uniqueId, time_out);
    }

    public byte[] submit(byte[] data) {
        String uniqueId = null;
        int time_out = 0;
        return submit(data, uniqueId, time_out);
    }

    public void shutdown() throws IllegalStateException {
        if (client == null) {
            throw new IllegalStateException("No client to shutdown");
        }
        client.shutdown();
    }

    public void close() throws IllegalStateException {
        if (client == null) {
            throw new IllegalStateException("No client to shutdown");
        }
        client.shutdown();
    }

}
