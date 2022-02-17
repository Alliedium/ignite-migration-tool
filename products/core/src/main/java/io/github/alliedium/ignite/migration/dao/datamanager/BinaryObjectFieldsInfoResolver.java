package io.github.alliedium.ignite.migration.dao.datamanager;

import io.github.alliedium.ignite.migration.util.BinaryObjectUtil;
import io.github.alliedium.ignite.migration.dao.converters.IIgniteBinaryDataConverter;
import io.github.alliedium.ignite.migration.dao.converters.IgniteEnumDataConverter;
import io.github.alliedium.ignite.migration.dao.converters.PlainObjectProviderDataConverter;

import java.util.*;

import io.github.alliedium.ignite.migration.dao.converters.TypesResolver;
import io.github.alliedium.ignite.migration.util.TypeUtils;
import org.apache.ignite.binary.BinaryObject;
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

    private FieldInfo createFieldInfo(BinaryObject fieldsContainer, String fieldName) {
        String fieldType = resolveType(fieldsContainer, fieldName);
        return createFieldInfo(fieldsContainer.field(fieldName), fieldType, fieldName);
    }

    private FieldInfo createFieldInfo(Object field, String fieldType, String fieldName) {
        Optional<Class<?>> fieldClazzOptional = TypeUtils.loadClassIfPossible(fieldType);
        IIgniteBinaryDataConverter fieldDataConverter;
        if (fieldClazzOptional.isPresent() && BinaryEnumObjectImpl.class.isAssignableFrom(fieldClazzOptional.get())) {
            fieldDataConverter = new IgniteEnumDataConverter();
        } else {
            fieldDataConverter = new PlainObjectProviderDataConverter();
        }

        Map<String, IFieldInfo> nested = new HashMap<>();
        if (TypeUtils.isBinaryObject(field)) {
            nested = extractFieldsInfo((BinaryObject) field);
        } else if (TypeUtils.isCollection(fieldType)) {
            nested = extractFieldsInfo((Collection<?>) field);
        } else if (TypeUtils.isMap(fieldType)) {
            nested = extractFieldsInfo((Map<?, ?>) field);
        }

        // todo: type info conversion from java to avro should be placed not here.
        fieldType = nested.isEmpty() ? TypesResolver.toAvroType(fieldType) : fieldType;

        return new FieldInfo.Builder()
                .setName(fieldName)
                .setTypeInfo(fieldType)
                .setNested(nested)
                .setBinaryObjectFieldDataConverter(fieldDataConverter)
                .build();
    }

    private Map<String, IFieldInfo> extractFieldsInfo(BinaryObject binaryObject) {
        Map<String, IFieldInfo> fieldsInfo = new HashMap<>();
        Collection<String> fieldNames = BinaryObjectUtil.isAffinityKey(binaryObject) ?
                affinityKeyFields : binaryObject.type().fieldNames();
        fieldNames.forEach(name ->
                fieldsInfo.put(name, createFieldInfo(binaryObject, name)));
        return fieldsInfo;
    }

    private Map<String, IFieldInfo> extractFieldsInfo(Collection<?> collection) {
        if (!collection.iterator().hasNext()) {
            return Collections.emptyMap();
        }
        Optional<?> optional = collection.stream().filter(Objects::nonNull).findFirst();
        if (!optional.isPresent()) {
            return Collections.emptyMap();
        }

        Object value = optional.get();
        String valueType = resolveType(value);
        Map<String, IFieldInfo> fieldsInfo = new HashMap<>();
        fieldsInfo.put(TypeUtils.VALUE, createFieldInfo(value, valueType, TypeUtils.VALUE));

        return fieldsInfo;
    }

    private Map<String, IFieldInfo> extractFieldsInfo(Map<?, ?> map) {
        if (map.isEmpty()) {
            return Collections.emptyMap();
        }
        Optional<?> optionalKey = map.keySet().stream().filter(Objects::nonNull).findFirst();
        Optional<?> optionalVal = map.values().stream().filter(Objects::nonNull).findFirst();
        if (!optionalKey.isPresent() || !optionalVal.isPresent()) {
            return Collections.emptyMap();
        }

        Object key = optionalKey.get();
        String keyType = resolveType(key);
        Object value = optionalVal.get();
        String valueType = resolveType(value);

        Map<String, IFieldInfo> fieldsInfo = new HashMap<>();
        fieldsInfo.put(TypeUtils.KEY, createFieldInfo(key, keyType, TypeUtils.KEY));
        fieldsInfo.put(TypeUtils.VALUE, createFieldInfo(value, valueType, TypeUtils.VALUE));

        return fieldsInfo;
    }

    private String resolveType(BinaryObject fieldsContainer, String fieldName) {
        if (TypeUtils.isBinaryObject(fieldsContainer.field(fieldName))) {
            return ((BinaryObject) fieldsContainer.field(fieldName)).type().typeName();
        }

        return fieldsContainer.field(fieldName) == null ?
                BinaryObjectUtil.isAffinityKey(fieldsContainer)
                        ? getAffinityKeyFieldType(fieldName)
                        : fieldsContainer.type().fieldTypeName(fieldName)
                : fieldsContainer.field(fieldName).getClass().getName();
    }

    private String resolveType(Object obj) {
        if (TypeUtils.isBinaryObject(obj)) {
            return ((BinaryObject) obj).type().typeName();
        }
        return obj.getClass().getName();
    }

    private String getAffinityKeyFieldType(String fieldName) {
        try {
            return AffinityKey.class.getDeclaredField(fieldName).getType().getName();
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
