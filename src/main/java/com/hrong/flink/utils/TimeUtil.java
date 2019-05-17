package com.hrong.flink.utils;

import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Description
 * @Author hrong
 * @Date 2019/5/16 13:57
 **/
public class TimeUtil {
	public static void outputTimeByCountAndOffset(int count, Time offset) {
		long now = System.currentTimeMillis();
		System.out.println("now time : " + now);
		long offsetSeconds = offset.toMilliseconds();
		for (int i = 1; i <= count; i++) {
			System.out.println(now + offsetSeconds * i);
		}
	}
	public static void mockData(){

	}
	public static void main(String[] args) {
		outputTimeByCountAndOffset(2, Time.seconds(1L));
	}
}
