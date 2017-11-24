package com.di.mesa.metric.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeanUtil {

	private static Logger logger = LoggerFactory.getLogger(BeanUtil.class);

	/*
	 * c:为赋值对象， values为给属性要赋的值，put('m1',1);put('m2',2),m1、m2等为属性
	 */
	public void setValue(Object object, Map<String, String> values) {
		Map<String, Field> fieldMap = new HashMap<String, Field>(); // 封装属性
		Map<String, Method> methodMap = new HashMap<String, Method>(); // 封装属性的set方法

		try {
			Field[] fields = object.getClass().getDeclaredFields();
			Method[] methods = object.getClass().getDeclaredMethods();

			for (Field field : fields) {
				String attri = field.getName();
				fieldMap.put(attri.toLowerCase(), field);
				for (Method method : methods) {
					String meth = method.getName();
					// 匹配set方法
					if (meth != null && "set".equals(meth.substring(0, 3)) && Modifier.isPublic(method.getModifiers())
							&& ("set" + Character.toUpperCase(attri.charAt(0)) + attri.substring(1)).equals(meth)) {
						methodMap.put(attri.toLowerCase(), method);
						break;
					}
				}
			}

			// 2、属性赋值
			for (Iterator it = values.keySet().iterator(); it.hasNext();) {
				String name = (String) it.next();
				String value = (String) values.get(name);

				if (value == null)
					continue;
				value = value.trim();
				name = name.trim();

				Field field = fieldMap.get(name.toLowerCase());
				if (field == null)
					continue;
				Method method = methodMap.get(name.toLowerCase());
				if (method == null)
					continue;
				fill(object, field, method, value);
			}

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	/**
	 * 将字符串值转换为合适的值填充到对象的指定域
	 * 
	 * @param bean
	 *            被填充的java bean
	 * @param field
	 *            需要填充的域
	 * @param value
	 *            字符串值
	 */
	private void fill(Object bean, Field field, Method method, String value) {
		if (value == null || "null".equalsIgnoreCase(value))
			return;

		try {
			Object[] oo = new Object[1];
			String type = field.getType().getName();
			if ("java.lang.String".equals(type)) {
				oo[0] = value;
			} else if ("java.lang.Integer".equals(type)) {
				if (value.length() > 0)
					oo[0] = Integer.valueOf(value);
			} else if ("java.lang.Float".equals(type)) {
				if (value.length() > 0)
					oo[0] = Float.valueOf(value);
			} else if ("java.lang.Double".equals(type)) {
				if (value.length() > 0)
					oo[0] = Double.valueOf(value);
			} else if ("java.math.BigDecimal".equals(type)) {
				if (value.length() > 0)
					oo[0] = new BigDecimal(value);
			} else if ("java.lang.Boolean".equals(type)) {
				if (value.length() > 0)
					oo[0] = Boolean.valueOf(value);
			} else if ("java.lang.Long".equals(type) || "long".equals(type)) {
				if (value.length() > 0)
					oo[0] = Long.valueOf(value);
			} else if ("java.sql.Timestamp".equals(type)) {
				if (value.length() > 0)
					oo[0] = Timestamp.valueOf(value);
			}

			method.invoke(bean, oo);

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
}
