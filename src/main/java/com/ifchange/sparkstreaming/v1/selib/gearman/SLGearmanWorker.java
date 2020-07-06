package com.ifchange.sparkstreaming.v1.selib.gearman;

import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;
import org.gearman.Gearman;
import org.gearman.GearmanServer;
import org.gearman.GearmanWorker;

import java.io.*;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SLGearmanWorker {
	private String cfile;
	private List<SLGearmanHost> hosts = new ArrayList<SLGearmanHost>();
	private int concurrency = 1;
	private String name;
	private Gearman gearman = null;
	private List<GearmanServer> servers = new ArrayList<GearmanServer>();
	private List<SLGearmanFunction> funcs = new ArrayList<SLGearmanFunction>();
	private List<GearmanWorker> workers = new ArrayList<GearmanWorker>();
	private static Logger logger=Logger.getLogger(SLGearmanWorker.class);

	public SLGearmanWorker(String cfile, String name, int concurrency) {
		this.name = name;
		this.cfile = cfile;
		this.concurrency = concurrency;
	}

	public void addWorker(SLGearmanFunction func) {
		func.setNameId(this.name, funcs.size());
		this.funcs.add(func);
	}

	public void start() throws Exception {
		this.hosts = parseHosts(this.cfile, this.name);
		gearman = Gearman.createGearman();
		for (SLGearmanHost host : hosts) {
			GearmanServer server = gearman.createGearmanServer(host.getIp(), host.getPort());
			servers.add(server);
		}
		for (int i = 0; i < funcs.size(); i++) {
			GearmanWorker worker = gearman.createGearmanWorker();
			worker.setMaximumConcurrency(concurrency);
			worker.addFunction(this.name, funcs.get(i));
			for (GearmanServer server : servers) {
				worker.addServer(server);
			}
			workers.add(worker);
		}
	}

	public void close() {
		for (GearmanWorker worker : workers) {
			worker.shutdown();
		}
		for (GearmanServer server : servers) {
			server.shutdown();
		}
		if (gearman != null) {
			gearman.shutdown();
		}
	}

	public void reset() throws Exception {
		close();
		start();
	}

	@SuppressWarnings("unchecked")
	public static List<SLGearmanHost> parseHosts(String cfile, String name) {
		List<SLGearmanHost> hosts = new ArrayList<SLGearmanHost>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(cfile)));
			String text = br.readLine();
			Map<String, Object> rootMap = (Map<String, Object>) JSON.parse(text);
			Map<String, Object> nameMap = (Map<String, Object>) rootMap.get(name);
			List<String> hostList = (List<String>) nameMap.get("host");
			for (String h : hostList) {
				SLGearmanHost host = new SLGearmanHost(h);
				hosts.add(host);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return hosts;
	}
	
	@SuppressWarnings("unchecked")
	public static List<SLGearmanHost> parseHosts2(String name) {
		List<SLGearmanHost> hosts = new ArrayList<>();
		BufferedReader br = null;
		try {
			/**
			 * update at 11/3 加载不到
			 */
//			br = new BufferedReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream("gm.conf")));
			br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/opt/hadoop/gm/gm.conf"))));
			String text = br.readLine();
			Map<String, Object> rootMap = (Map<String, Object>) JSON.parse(text);
			Map<String, Object> nameMap = (Map<String, Object>) rootMap.get(name);
			List<String> hostList = (List<String>) nameMap.get("host");
			for (String h : hostList) {
				SLGearmanHost host = new SLGearmanHost(h);
				hosts.add(host);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return hosts;
	}

	@SuppressWarnings("unchecked")
	public static String getLocalWorkerName(String cfile, String prefix) {
		BufferedReader br = null;
		try {
			// 本机IP
			InetAddress addr = InetAddress.getLocalHost();
			String ip = addr.getHostAddress();
			// 通过IP找本机workername
			br = new BufferedReader(new InputStreamReader(new FileInputStream(cfile)));
			String text = br.readLine();
			Map<String, Object> rootMap = (Map<String, Object>) JSON.parse(text);
			Iterator<Entry<String, Object>> iter = rootMap.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, Object> entry = iter.next();
				String name = entry.getKey();
				Map<String, Object> nameMap = (Map<String, Object>) entry.getValue();
				List<String> list = (List<String>) nameMap.get("host");
				if (list.size() != 1)
					continue;
				SLGearmanHost host = new SLGearmanHost(list.get(0));
				if (host.getIp().equalsIgnoreCase(ip) && name.startsWith(prefix)) {
					return name;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	public String getName() {
		return name;
	}

	
}
