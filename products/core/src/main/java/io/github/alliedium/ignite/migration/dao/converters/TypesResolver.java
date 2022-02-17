package io.github.alliedium.ignite.migration.dao.converters;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class TypesResolver {

    private static final Map<String, String> javaToAvroTypes = new HashMap<>();
    private static final String BooleanClassName = Boolean.class.getName();
    private static final String StringClassName = String.class.getName();
    private static final String IntegerClassName = Integer.class.getName();
    private static final String LongClassName = Long.class.getName();
    private static final String FloatClassName = Float.class.getName();
    private static final String DoubleClassName = Double.class.getName();
    private static final String ByteArrayClassName = byte[].class.getName();
    private static final String TimestampClassName = Timestamp.class.getName();

    static {
        javaToAvroTypes.put(BooleanClassName, "boolean"); //Schema.Type.BOOLEAN
        javaToAvroTypes.put(StringClassName, "string"); //Schema.Type.STRING
        javaToAvroTypes.put(IntegerClassName, "int"); //Schema.Type.INT
        javaToAvroTypes.put(LongClassName, "long"); //Schema.Type.LONG
        javaToAvroTypes.put(FloatClassName, "float"); //Schema.Type.FLOAT
        javaToAvroTypes.put(DoubleClassName, "double"); //Schema.Type.DOUBLE
        javaToAvroTypes.put(ByteArrayClassName, "byte[]"); //Schema.Type.BYTES
        javaToAvroTypes.put(TimestampClassName, "timestamp");
    }

    public static String toAvroType(String clazzName) {
        String type = javaToAvroTypes.get(clazzName);
        return type == null ? convertJavaTypeToAvro(clazzName) : type;
    }

    private static String convertJavaTypeToAvro(String clazzName) {
        return clazzName.substring(clazzName.lastIndexOf(".") + 1).toLowerCase();
    }

    public static boolean isTimestamp(String type) {
        return type != null && javaToAvroTypes.get(TimestampClassName).equals(type.toLowerCase());
    }

    public static boolean isByteArray(String type) {
        return javaToAvroTypes.get(ByteArrayClassName).equals(type);
    }

    public static boolean isString(String type) {
        return javaToAvroTypes.get(StringClassName).equals(type);
    }

    public static boolean isInteger(String type) {
        return javaToAvroTypes.get(IntegerClassName).equals(type);
    }

    public static String getTypeTimestamp() {
        return javaToAvroTypes.get(TimestampClassName);
    }

    public static String getTypeByteArray() {
        return javaToAvroTypes.get(ByteArrayClassName);
    }
}
