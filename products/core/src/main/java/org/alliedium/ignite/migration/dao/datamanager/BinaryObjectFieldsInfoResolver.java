package org.alliedium.ignite.migration.dao.datamanager;

import org.alliedium.ignite.migration.dao.converters.IIgniteBinaryDataConverter;
import org.alliedium.ignite.migration.dao.converters.IgniteEnumDataConverter;
import org.alliedium.ignite.migration.dao.converters.PlainObjectProviderDataConverter;

import java.util.*;

import org.alliedium.ignite.migration.dao.converters.TypesResolver;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;

/**
 * Unit responsible for figuring out the meta-data of Apache Ignite cache fields (field name, value type class name,
 * converter needed to get a common Java representation when the field value is of Apache Ignite specific class).
 * Meta-data required for Apache Ignite cache fields serialization is being defined from {@link BinaryObject} and {@link org.apache.ignite.cache.QueryEntity}.
 * Resulting output is represented as a list of {@link FieldInfo}.
 */
public class BinaryObjectFieldsInfoResolver implements IBinaryObjectFieldInfoResolver {

    private static final List<String> affinityKeyFields = Collections.unmodifiableList(Arrays.asList("key", "affKey"));

    private final BinaryObject binaryObject;

    public BinaryObjectFieldsInfoResolver(BinaryObject binaryObject) {
        this.binaryObject = binaryObject;
    }

    @Override
    public Map<String, IFieldInfo> resolveFieldsInfo() {
        return extractFieldsInfo(binaryObject);
    }

    private FieldInfo createFieldInfo(BinaryObject binaryObject, String fieldName) {
        String typeInfo = resolveType(binaryObject, fieldName);
        Optional<Class<?>> fieldClazzOptional = loadClassIfPossible(typeInfo);

        IIgniteBinaryDataConverter fieldDataConverter;
        if (fieldClazzOptional.isPresent() && BinaryEnumObjectImpl.class.isAssignableFrom(fieldClazzOptional.get())) {
            fieldDataConverter = new IgniteEnumDataConverter();
        } else {
            fieldDataConverter = new PlainObjectProviderDataConverter();
        }

        Map<String, IFieldInfo> nested = new HashMap<>();
        if (isBinaryObject(binaryObject.field(fieldName))) {
            nested = extractFieldsInfo(binaryObject.field(fieldName));
        }

        // todo: type info conversion from java to avro should be placed not here.
        typeInfo = nested.isEmpty() ? TypesResolver.toAvroType(typeInfo) : typeInfo;

        return new FieldInfo.Builder()
                .setName(fieldName)
                .setTypeInfo(typeInfo)
                .setNested(nested)
                .setBinaryObjectFieldDataConverter(fieldDataConverter)
                .build();
    }

    private Map<String, IFieldInfo> extractFieldsInfo(BinaryObject binaryObject) {
        Map<String, IFieldInfo> fieldsInfo = new HashMap<>();
        Collection<String> fieldNames = isAffinityKey(binaryObject) ?
                affinityKeyFields : binaryObject.type().fieldNames();
        fieldNames.forEach(name ->
                fieldsInfo.put(name, createFieldInfo(binaryObject, name)));
        return fieldsInfo;
    }

    private String resolveType(BinaryObject binaryObject, String fieldName) {
        if (isBinaryObject(binaryObject.field(fieldName))) {
            return ((BinaryObject) binaryObject.field(fieldName)).type().typeName();
        }

        return binaryObject.field(fieldName) == null ?
                isAffinityKey(binaryObject)
                        ? getAffinityKeyFieldType(fieldName)
                        : binaryObject.type().fieldTypeName(fieldName)
                : binaryObject.field(fieldName).getClass().getName();
    }

    private String getAffinityKeyFieldType(String fieldName) {
        try {
            return AffinityKey.class.getDeclaredField(fieldName).getType().getName();
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private Optional<Class<?>> loadClassIfPossible(String className) {
        try {
            return Optional.of(Class.forName(className));
        } catch (ClassNotFoundException e) {
            // do nothing
        }
        return Optional.empty();
    }

    private boolean isAffinityKey(BinaryObject binaryObject) {
        try {
            binaryObject.type().fieldNames();
            return binaryObject.type().typeName().equals(AffinityKey.class.getName());
        } catch (BinaryObjectException e) {
            return affinityKeyFields.stream().allMatch(binaryObject::hasField);
        }
    }

    private boolean isBinaryObject(Object obj) {
        return obj != null && BinaryObject.class.isAssignableFrom(obj.getClass());
    }
}
