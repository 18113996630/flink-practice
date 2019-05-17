package com.hrong.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @Description
 * @Author hrong
 * @Date 2019/5/17 18:20
 **/
public class TableQueryJava {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
//		env.fromElements()
	}
}
