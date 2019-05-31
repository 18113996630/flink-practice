package com.hrong.flink.utils;

/**
 * @Author hrong
 * @ClassName StringTest
 * @Description
 * @Date 2019/5/31 13:01
 **/
public class StringTest {
	public static void main(String[] args) {
		String b = "a";
		String s = new String("a");
		System.out.println(b == s);
		System.out.println(s.hashCode());
		System.out.println(b.hashCode());
	}
}
