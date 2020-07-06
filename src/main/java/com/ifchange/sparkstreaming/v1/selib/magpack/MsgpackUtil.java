package com.ifchange.sparkstreaming.v1.selib.magpack;

import org.msgpack.template.Templates;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Converter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MsgpackUtil {

	public static Object valueToObject(Value value) throws IOException {
		if (value == null || value.isNilValue()) {
			return null;
		} else if (value.isBooleanValue()) {
			return value.asBooleanValue().getBoolean();
		} else if (value.isFloatValue()) {
			return value.asFloatValue().getDouble();
		} else if (value.isIntegerValue()) {
			return value.asIntegerValue().getLong();
		} else if (value.isArrayValue()) {
			Converter converter = new Converter(value);
			List<Value> subValueList = converter.read(Templates.tList(Templates.TValue));
			List<Object> list = new ArrayList<Object>();
			for (Value subValue : subValueList) {
				Object o = valueToObject(subValue);
				if (o != null)
					list.add(o);
			}
			converter.close();
			return list;
		} else if (value.isMapValue()) {
			Converter converter = new Converter(value);
			Map<Value, Value> subValueMap = converter.read(Templates.tMap(Templates.TValue, Templates.TValue));
			Map<Object, Object> map = new LinkedHashMap<Object, Object>();
			for (Entry<Value, Value> entry : subValueMap.entrySet()) {
				Object ekey = valueToObject(entry.getKey());
				Object evalue = valueToObject(entry.getValue());
				if (ekey != null && evalue != null) {
					map.put(ekey, evalue);
				}
			}
			converter.close();
			return map;
		} else if (value.isRawValue()) {
			Converter converter = new Converter(value);
			String str = converter.read(Templates.TString);
			converter.close();
			return str;
		} else {
			return null;
		}
	}

}
