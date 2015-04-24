package com.github.gbraccialli.telco.cdr.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class Test {

	public static void main(String[] args) throws Exception{
		timestampRounding();

	}

	
	
	public static void timestampRounding() throws ParseException{
		
		System.out.println("teste");
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println(new Date(roundDate(dateFormat.parse("2015-01-01 00:00:01").getTime(),3*60*1000)));
		System.out.println(new Date(roundDate(dateFormat.parse("2015-01-01 00:01:01").getTime(),3*60*1000)));
		System.out.println(new Date(roundDate(dateFormat.parse("2015-01-01 00:02:01").getTime(),3*60*1000)));
		System.out.println(new Date(roundDate(dateFormat.parse("2015-01-01 00:03:01").getTime(),3*60*1000)));
		System.out.println(new Date(roundDate(dateFormat.parse("2015-01-01 00:04:01").getTime(),3*60*1000)));
		System.out.println(new Date(roundDate(dateFormat.parse("2015-01-01 00:05:01").getTime(),3*60*1000)));
		System.out.println(new Date(roundDate(dateFormat.parse("2015-01-01 00:06:01").getTime(),3*60*1000)));
		System.out.println(new Date(roundDate(dateFormat.parse("2015-01-01 00:07:01").getTime(),3*60*1000)));
		System.out.println(new Date(roundDate(dateFormat.parse("2015-01-01 00:08:01").getTime(),3*60*1000)));
		
		
		
	}
	
	public static long roundDate(long miliseconds, long interval){
		return miliseconds / interval * interval;
	}
	
}
