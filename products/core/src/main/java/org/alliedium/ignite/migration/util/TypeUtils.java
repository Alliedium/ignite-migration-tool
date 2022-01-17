package org.alliedium.ignite.migration.util;

import org.apache.ignite.binary.BinaryObject;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public class TypeUtils {

    public static final String VALUE = "value";
    public static final String KEY = "key";
    public static final String FIELD_TYPE = "fieldType";

    public static boolean isBinaryObject(Object obj) {
        return obj != null && BinaryObject.class.isAssignableFrom(obj.getClass());
    }

    public static boolean isCollection(String typeInfo) {
        Optional<Class<?>> classOptional = loadClassIfPossible(typeInfo);
        return classOptional.filter(Collection.class::isAssignableFrom).isPresent();
    }

    public static boolean isMap(String typeInfo) {
        Optional<Class<?>> classOptional = loadClassIfPossible(typeInfo);
        return classOptional.filter(Map.class::isAssignableFrom).isPresent();
    }

    public static Optional<Class<?>> loadClassIfPossible(String className) {
        try {
            return Optional.of(Class.forName(className));
        } catch (ClassNotFoundException e) {
            return Optional.empty();
        }
    }
}
