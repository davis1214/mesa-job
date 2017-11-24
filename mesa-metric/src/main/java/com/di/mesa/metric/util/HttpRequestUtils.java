/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.di.mesa.metric.util;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import com.di.mesa.metric.common.ExecutorManagerException;
import org.apache.commons.lang.StringUtils;


public class HttpRequestUtils {

    /**
     * parse a string as number and throws exception if parsed value is not a
     * valid integer
     *
     * @param params
     * @param paramName
     * @throws ExecutorManagerException if paramName is not a valid integer
     */
    public static boolean validateIntegerParam(Map<String, String> params, String paramName)
            throws ExecutorManagerException {
        if (params != null && params.containsKey(paramName) && !StringUtils.isNumeric(params.get(paramName))) {
            throw new ExecutorManagerException(paramName + " should be an integer");
        }
        return true;
    }

    /**
     * Checks for the existance of the parameter in the request
     *
     * @param request
     * @param param
     * @return
     */
    public static boolean hasParam(HttpServletRequest request, String param) {
        return request.getParameter(param) != null;
    }

    /**
     * Retrieves the param from the http servlet request. Will throw an
     * exception if not found
     *
     * @param request
     * @param name
     * @return
     * @throws ServletException
     */
    public static String getParam(HttpServletRequest request, String name) throws ServletException {
        String p = request.getParameter(name);
        if (p == null) {
            throw new ServletException("Missing required parameter '" + name + "'.");
        } else {
            return p;
        }
    }

    /**
     * Retrieves the param from the http servlet request.
     *
     * @param request
     * @param name
     * @param defaultVal
     * @return
     */
    public static String getParam(HttpServletRequest request, String name, String defaultVal) {
        String p = request.getParameter(name);
        if (p == null) {
            return defaultVal;
        }
        return p;
    }

    /**
     * Returns the param and parses it into an int. Will throw an exception if
     * not found, or a parse error if the type is incorrect.
     *
     * @param request
     * @param name
     * @return
     * @throws ServletException
     */
    public static int getIntParam(HttpServletRequest request, String name) throws ServletException {
        String p = getParam(request, name);
        return Integer.parseInt(p);
    }

    public static int getIntParam(HttpServletRequest request, String name, int defaultVal) {
        if (hasParam(request, name)) {
            try {
                return getIntParam(request, name);
            } catch (Exception e) {
                return defaultVal;
            }
        }

        return defaultVal;
    }

    public static boolean getBooleanParam(HttpServletRequest request, String name) throws ServletException {
        String p = getParam(request, name);
        return Boolean.parseBoolean(p);
    }

    public static boolean getBooleanParam(HttpServletRequest request, String name, boolean defaultVal) {
        if (hasParam(request, name)) {
            try {
                return getBooleanParam(request, name);
            } catch (Exception e) {
                return defaultVal;
            }
        }

        return defaultVal;
    }

    public static long getLongParam(HttpServletRequest request, String name) throws ServletException {
        String p = getParam(request, name);
        return Long.valueOf(p);
    }

    public static long getLongParam(HttpServletRequest request, String name, long defaultVal) {
        if (hasParam(request, name)) {
            try {
                return getLongParam(request, name);
            } catch (Exception e) {
                return defaultVal;
            }
        }

        return defaultVal;
    }

    public static Map<String, String> getParamGroup(HttpServletRequest request, String groupName)
            throws ServletException {
        @SuppressWarnings("unchecked")
        Enumeration<Object> enumerate = (Enumeration<Object>) request.getParameterNames();
        String matchString = groupName + "[";

        HashMap<String, String> groupParam = new HashMap<String, String>();
        while (enumerate.hasMoreElements()) {
            String str = (String) enumerate.nextElement();
            if (str.startsWith(matchString)) {
                groupParam.put(str.substring(matchString.length(), str.length() - 1), request.getParameter(str));
            }

        }
        return groupParam;
    }

}
