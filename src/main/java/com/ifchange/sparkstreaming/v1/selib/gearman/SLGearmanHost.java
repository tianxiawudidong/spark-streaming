package com.ifchange.sparkstreaming.v1.selib.gearman;

public class SLGearmanHost {
	private String ip;
	private int port;

	public SLGearmanHost(String str) {
		String[] col = str.split(":");
		ip = col[0];
		port = Integer.valueOf(col[1]);
	}

	public String toString() {
		return ip + ":" + port;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
}
