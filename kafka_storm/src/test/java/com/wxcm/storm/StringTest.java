package com.wxcm.storm;

public class StringTest {
	public static void main(String[] args) {
		String str = "abc";
		StringBuffer buff = new StringBuffer();
		str = buff.append(str).reverse().toString();
		System.out.println(str);
	}
}
