package com.example.hazelcastdemo.util;

public class HazelUtil {

	public static String getepoch() {
		long epoch = System.currentTimeMillis();
		return String.valueOf(epoch / 10);
	}
}
