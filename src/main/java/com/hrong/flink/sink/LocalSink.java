package com.hrong.flink.sink;

import com.hrong.flink.model.StudentJava;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description
 * @Author hrong
 * @Date 2019/5/9 11:45
 **/
public class LocalSink {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<StudentJava> streamSource = env.fromElements(new StudentJava(1, 2, "cracy", 20),
				new StudentJava(2, 2, "leecon", 22));
		streamSource.print();
		streamSource.printToErr();
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
