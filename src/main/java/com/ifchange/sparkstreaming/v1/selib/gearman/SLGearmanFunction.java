package com.ifchange.sparkstreaming.v1.selib.gearman;

import org.gearman.GearmanFunction;
import org.gearman.GearmanFunctionCallback;

public abstract class SLGearmanFunction implements GearmanFunction {
	private String func_name = "";
	private int func_id = 0;

	public void setNameId(String func_name, int func_id) {
		this.func_name = func_name;
		this.func_id = func_id;
	}

	@Override
	public byte[] work(String arg0, byte[] arg1, GearmanFunctionCallback arg2) throws Exception {
		log(func_name, func_id);
		return dowork(arg0, arg1, arg2);
	}

	abstract public void log(String func_name, int func_id);

	abstract public byte[] dowork(String arg0, byte[] arg1, GearmanFunctionCallback arg2) throws Exception;
}
