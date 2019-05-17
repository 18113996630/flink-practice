package com.hrong.flink.utils;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

/**
 * @Description
 * @Author hrong
 * @Date 2019/5/14 11:31
 **/
public class ConsumerDeserializationSchema<T> implements DeserializationSchema<T> {

	private Class<T> clazz;

	public ConsumerDeserializationSchema(Class<T> clazz) {
		this.clazz = clazz;
	}

	@Override
	public T deserialize(byte[] bytes) throws IOException {
		T res = null;
		try {
			res = JSON.parseObject(new String(bytes), clazz);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
	}

	@Override
	public boolean isEndOfStream(T t) {
		return false;
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return TypeExtractor.getForClass(clazz);
	}
}
