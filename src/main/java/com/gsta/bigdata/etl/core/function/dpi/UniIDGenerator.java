package com.gsta.bigdata.etl.core.function.dpi;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;

/**
 * @author xiangy
 *
 */
public class UniIDGenerator {
	private static StringBuffer gsnBuff = new StringBuffer(100); 
	private static long counter = 0;
	private static String uniIDPrefix = "";

	static {
		try {
			//获取当前所在主机的IP地址
			String ipAddrs = "127.0.0.1";
			InetAddress addr = InetAddress.getLocalHost();
			ipAddrs = addr.getHostAddress().toString();
			
			//获取进程ID
			String name = ManagementFactory.getRuntimeMXBean().getName();
			uniIDPrefix += ipAddrs;
			uniIDPrefix += name.split("@")[0];
		} catch (Exception e) {
		}
	}
	
	/**
	 * @return
	 */
	public static String generateUniqueID() {
		gsnBuff.setLength(0);
		gsnBuff.append(uniIDPrefix);
		gsnBuff.append(System.currentTimeMillis());
		gsnBuff.append(Thread.currentThread().getId());
		gsnBuff.append((++counter));
		return gsnBuff.toString();
	}
}
