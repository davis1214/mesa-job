//package com.di.monitor.com.di.mesa.metric.common;
//
//import java.net.InetAddress;
//import java.net.SocketException;
//import java.net.UnknownHostException;
//
//import com.di.monitor.com.di.mesa.metric.util.Utils;
//
//public class IPTest {
//
//	public static void main(String[] args) throws UnknownHostException, SocketException {
//
//		String hostAddress = InetAddress.getLocalHost().getHostAddress();
//
//		System.out.println(hostAddress);
//
//		System.out.println(Utils.getHostIp());
//
//		String[] ss = "sss".split(",");
//		System.out.println(ss.length);
//
//		IPTest test = new IPTest();
//		test.addPrintDataType(10);
//		System.out.println("printDataType:" + test.printDataType);
//
//	}
//
//	private int printDataType = 0;
//
//	public void addPrintDataType(int newValue) {
//		this.printDataType = this.printDataType | newValue;
//	}
//
//}
