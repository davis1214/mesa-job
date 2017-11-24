package com.di.mesa.metric.mbean;

import java.lang.management.ManagementFactory;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;

public class JmxMBeanManager {
    private static final Object mbsCreateMonitor = new Object();
    private static JmxMBeanManager thisObj;

    private final MBeanServer mbs;

    public JmxMBeanManager() {
        mbs = ManagementFactory.getPlatformMBeanServer();
    }

    public static JmxMBeanManager getInstance() {
        synchronized (mbsCreateMonitor) {
            if (null == thisObj) {
                thisObj = new JmxMBeanManager();
            }
        }
        return thisObj;
    }

    public boolean isRegistered(String name) {
        try {
            ObjectName objName = new ObjectName(name);
            return mbs.isRegistered(objName);
        } catch (Exception e) {
            throw new RuntimeException("exception while checking if MBean is registered, " + name, e);
        }
    }

    @SuppressWarnings("rawtypes")
    private final ConcurrentHashMap<Class, AtomicInteger> classInstanceCounter = new ConcurrentHashMap<Class, AtomicInteger>();

    @SuppressWarnings("rawtypes")
    private int incrAndGet(Class Key) {
        return incrAndGet(Key, 1);
    }

    @SuppressWarnings("rawtypes")
    private int incrAndGet(Class key, final int incrValue) {
        AtomicInteger bookIdCount = classInstanceCounter.get(key);
        if (bookIdCount == null) {
            AtomicInteger bookIdPrev = classInstanceCounter.putIfAbsent(key, new AtomicInteger(incrValue));
            if (bookIdPrev != null) {
                return bookIdPrev.addAndGet(incrValue);
            } else {
                return incrValue;
            }
        } else {
            return bookIdCount.addAndGet(incrValue);
        }
    }

    public void appendMBean(Object theBean) {
        appendMBean(theBean, null, 6);
    }

    /**
     * @param simpleName NULL的时，系统自动提取类名简称。
     */
    public void appendMBean(Object theBean, String simpleName) {
        if (simpleName == null || simpleName.length() > 20) {
            throw new IllegalArgumentException();
        }
        appendMBean(theBean, simpleName, -1);
    }

    public void appendMBean(Object theBean, String simpleName, int prefix) {
        String beanName = genBeanName(theBean, simpleName, prefix);
        if (JmxMBeanManager.getInstance().isRegistered(beanName)) {
            JmxMBeanManager.getInstance().unregisterMBean(beanName);
        }
        JmxMBeanManager.getInstance().registerMBean(theBean, beanName);
    }

    public void appendMBean(Object theBean, String simpleName, boolean unique) {
        String beanName = genBeanName(theBean, simpleName, -1);
        if (unique) {
            beanName = beanName.substring(0, beanName.lastIndexOf("-"));
        }
        if (JmxMBeanManager.getInstance().isRegistered(beanName)) {
            JmxMBeanManager.getInstance().unregisterMBean(beanName);
        }
        JmxMBeanManager.getInstance().registerMBean(theBean, beanName);
    }

    @SuppressWarnings("rawtypes")
    protected String genBeanName(Object theBean, String simpleName, int prefix) {
        Class beanClass = theBean.getClass();
        String packageName = beanClass.getPackage().getName();

        String prefixName = (prefix < 0 ? beanClass.getName() : packagePrefix(beanClass, prefix));
        String typeName = (simpleName == null ? beanClass.getSimpleName() : simpleName);

        // String prefixName = beanClass.getName();
        // String typeName = beanClass.getSimpleName();

        int instanceNum = incrAndGet(beanClass);
        String beanName = packageName + ":type=" + typeName + "-" + instanceNum;
        // String beanName = prefixName + ":type=" + typeName + "-" + instanceNum;
        return beanName;
    }

    private static String packagePrefix(String className, int maxDot) {
        int fromIndex = -1;
        int lastIndex = 0;
        int iteration = 0;
        do {
            lastIndex = fromIndex;
            fromIndex = className.indexOf(".", fromIndex + 1);
            iteration++;
        } while (iteration < maxDot && fromIndex != -1);

        if (lastIndex <= 0) {
            return className;
        }
        return className.substring(0, lastIndex);
    }

    @SuppressWarnings("rawtypes")
    private static String packagePrefix(Class classArg, int maxDot) {
        return packagePrefix(classArg.getName(), maxDot);
    }

    private void registerMBean(Object theBean, String name) {
        try {
            ObjectName objName = new ObjectName(name);
            mbs.registerMBean(theBean, objName);
        } catch (Exception e) {
            throw new RuntimeException("exception while registering MBean, " + name, e);
        }
    }

    private void unregisterMBean(String name) {
        try {
            ObjectName objName = new ObjectName(name);
            mbs.unregisterMBean(objName);
        } catch (Exception e) {
            throw new RuntimeException("exception while unregistering MBean, " + name, e);
        }
    }

    public Object getAttribute(String objName, String attrName) {
        try {
            ObjectName on = new ObjectName(objName);
            return mbs.getAttribute(on, attrName);
        } catch (Exception e) {
            throw new RuntimeException("exception while getting MBean attribute, " + objName + ", " + attrName, e);
        }
    }

    public Integer getIntAttribute(String objName, String attrName) {
        return (Integer) getAttribute(objName, attrName);
    }

    public String getStringAttribute(String objName, String attrName) {
        return (String) getAttribute(objName, attrName);
    }
}
