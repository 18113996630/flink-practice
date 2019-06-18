package com.hrong.flink.source;

import com.hrong.flink.model.StudentJava;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @Description
 * @Author huangrong
 * @Date 2019/5/8 23:36
 **/
public class MysqlSourceTest {
	public static void main(String[] args) {
		Logger.getLogger("").setLevel(Level.WARNING);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<StudentJava> dataStreamSource = env.addSource(new MysqlSource());
		dataStreamSource.print();
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
