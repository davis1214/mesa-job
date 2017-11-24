package com.di.mesa.metric.common;

import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class DateTest {

	public static void main(String[] args) throws ParseException {

		SimpleDateFormat s = new SimpleDateFormat("G YYYY MMM dd HH:mm:SS E D F k K z",Locale.US);
		System.out.println(s.format(new Date()));
        Date date = new Date("31-Aug-2016 09:50:03 UTC");
        String a = s.format(date);
        System.out.println(a);
        
       // System.out.println(s.parse("31-Aug-2016 09:50:03 UTC"));
        
       // String ss  = DateFormat.getDateInstance().format("31-Aug-2016 09:50:03 UTC");
       // System.out.println(ss);

        SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss",Locale.US);
		Date parse = sdf.parse("1-Aug-2016 09:50:03");
		
		SimpleDateFormat sdsf = new SimpleDateFormat("yyyyMMyyyy HH:mm:ss",Locale.US);
		//sdsf.format(new Date())
		
		
		
		//Wed Aug 31 09:50:03 CST 2016
		System.out.println("parse->"+parse);
		
       // Date parse = DateFormat.getDateInstance().parse("31-Aug-2016 09:50:03");
       // System.out.println(parse);
        
		System.out.println(new Date(-1));
		
		//需要 Locale 来执行其任务的操作称为语言环境敏感的 操作，它使用 Locale 为用户量身定制信息
		System.out.println(NumberFormat.getCurrencyInstance(Locale.CHINA).getCurrency().toString());
		System.out.println(NumberFormat.getCurrencyInstance(Locale.US).getCurrency().toString());
		System.out.println(NumberFormat.getCurrencyInstance(Locale.ENGLISH).getCurrency().toString());
		System.out.println(NumberFormat.getCurrencyInstance(Locale.GERMANY).getCurrency().toString());
		System.out.println(NumberFormat.getCurrencyInstance(Locale.GERMANY).getMinimumFractionDigits());
		
		
		System.out.println(new Date(1472982467000l));
		System.out.println(new Date(1472982537000l));
		
		
		
		SimpleDateFormat dddd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",Locale.US);
		
		
		System.out.println(dddd.parse("2016-09-19 11:40:37.180"));
		
	}

}
