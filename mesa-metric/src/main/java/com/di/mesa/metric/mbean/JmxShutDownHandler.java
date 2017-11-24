package com.di.mesa.metric.mbean;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class JmxShutDownHandler {

	final String objectName = System.getProperty("monitor.server.object.name");
	final String rmiServerHost = System.getProperty("monitor.server.jmx.host");
	final int rmiServerPort = Integer.parseInt(System.getProperty("monitor.server.jmx.port", "9999"));
	final String rmiServerJMX = "jmxrmi";
	final String jmxURLText;

	public JmxShutDownHandler() {
		this.jmxURLText = String.format("service:jmx:rmi:///jndi/rmi://%s:%d/%s", rmiServerHost, rmiServerPort,
				rmiServerJMX);
	}

	public void shutdown() {
		try {
			JMXServiceURL jmxURL = new JMXServiceURL(jmxURLText);
			Map<String, Object> enviMap = new HashMap<String, Object>();
			JMXConnector connector = JMXConnectorFactory.connect(jmxURL, enviMap);

			MBeanServerConnection conn = connector.getMBeanServerConnection();
			ObjectName mName = new ObjectName(objectName);
			conn.invoke(mName, "shutdown", null, null);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		JmxShutDownHandler handler = new JmxShutDownHandler();
		handler.shutdown();
		System.exit(0);
	}

}
