package org.alliedium.ignite.migration.test;

import java.lang.reflect.Method;

public class TestUtils {

    public static String getMethodName(Class<?> clazz, Method method) {
        return clazz.getName() + "$" + method;
    }
}
