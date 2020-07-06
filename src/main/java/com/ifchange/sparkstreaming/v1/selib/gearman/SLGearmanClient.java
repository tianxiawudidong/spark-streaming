package com.ifchange.sparkstreaming.v1.selib.gearman;

import org.gearman.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class SLGearmanClient {

    static final Logger logger = Logger.getLogger(SLGearmanClient.class.getName());

    public class GmAsyncClient implements GearmanJobEventCallback<String> {

        private byte[] output;

        public byte[] getOutput() {
            return output;
        }

        @Override
        public void onEvent(String attachment, GearmanJobEvent event) {
            switch (event.getEventType()) {
                case GEARMAN_JOB_SUCCESS: // Job completed successfully
                    output = event.getData();
                    break;
                case GEARMAN_SUBMIT_FAIL: // The job submit operation failed
                case GEARMAN_JOB_FAIL: // The job's execution failed
                default:
            }
        }
    }

    private String cfile;
    private String name;
    private Gearman gearman = null;
    private GearmanClient client = null;
    private List<SLGearmanHost> hosts = new ArrayList<SLGearmanHost>();
    private List<GearmanServer> servers = new ArrayList<GearmanServer>();

    public SLGearmanClient(String cfile, String name) {
        this.cfile = cfile;
        this.name = name;
    }
    public SLGearmanClient(String name) {
        this.name = name;
    }
    public void start() {
        try {
            hosts = SLGearmanWorker.parseHosts2(name);
            gearman = Gearman.createGearman();
            for (SLGearmanHost host : hosts) {
                GearmanServer server = gearman.createGearmanServer(host.getIp(), host.getPort());
                servers.add(server);
            }
            client = gearman.createGearmanClient();
            for (GearmanServer server : servers) {
                client.addServer(server);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            client.shutdown();
        } catch (Exception e) {
            logger.warning("shutdown client exception:" + e);
        }
        if (servers != null) {
            for (GearmanServer server : servers) {
                try {
                    server.shutdown();
                } catch (Exception e) {
                    logger.warning("shutdown server exception:" + e);
                }
            }
            servers.clear();
        }
        try {
            gearman.shutdown();
        } catch (Exception e) {
            logger.warning("shutdown gearman exception:" + e);
        }
    }

    public void reboot() {
        close();
        start();
    }

    public byte[] sync_call(byte[] input, long timeout) {
        if (client == null) {
            logger.warning("gearman client is not start......");
        }
        GearmanJobReturn jobReturn = client.submitJob(this.name, input);
        while (!jobReturn.isEOF()) {
            GearmanJobEvent event = null;
            try {
                event = jobReturn.poll(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
            }
            if (event == null) {
                return null;
            }
            switch (event.getEventType()) {
                case GEARMAN_JOB_SUCCESS:
                    return event.getData();
                case GEARMAN_SUBMIT_FAIL:
                case GEARMAN_JOB_FAIL:
                default:
            }
        }
        return null;
    }

    public byte[] async_call(byte[] input, long timeout, boolean wait) {
        GmAsyncClient callback = new GmAsyncClient();
        GearmanJoin<String> join = client.submitJob(name, input, name, callback);
        if (wait) {
            try {
                join.join(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return callback.getOutput();
    }
}
