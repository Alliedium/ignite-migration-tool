package org.alliedium.ignite.migration.dao.converters;

import org.alliedium.ignite.migration.dao.datamanager.IFieldInfo;
import org.alliedium.ignite.migration.dto.CacheEntryValue;
import org.alliedium.ignite.migration.dto.CacheEntryValueField;
import org.alliedium.ignite.migration.dto.ICacheEntryValue;
import org.alliedium.ignite.migration.dto.ICacheEntryValueField;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import org.alliedium.ignite.migration.util.TypeUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;

/**
 * Unit executes conversion from an Apache Ignite {@link BinaryObject} to {@link ICacheEntryValue} and vice-versa.
 * When converting from {@link BinaryObject}, unit needs to be provided with meta information for each BinaryObject field
 * (as a list of {@link IFieldInfo}) on initialization.
 * When converting from {@link ICacheEntryValue}, unit needs to be provided with correspondent Apache Ignite connection
 * and value type of an Apache Ignite cache, which BinaryObject is being built for.
 */
public class BinaryObjectConverter implements IIgniteDTOConverter<ICacheEntryValue, BinaryObject> {

    private Map<String, IFieldInfo> binaryObjectFieldsInfo;
    private Ignite ignite;
    private String cacheValueType;

    public BinaryObjectConverter(Map<String, IFieldInfo> cacheFieldsMetaData) {
        this.binaryObjectFieldsInfo = new HashMap<>(cacheFieldsMetaData);
    }

    public BinaryObjectConverter(Ignite ignite, String cacheValueType) {
        this.ignite = ignite;
        this.cacheValueType = cacheValueType;
    }

    public ICacheEntryValue convertFromEntity(BinaryObject binaryObject) {
        return new CacheEntryValue(parseBinaryObject(binaryObject, binaryObjectFieldsInfo));
    }

    public BinaryObject convertFromDTO(ICacheEntryValue dto) {
        BinaryObjectBuilder binaryObjectBuilder = ignite.binary().builder(cacheValueType);
        dto.getFields().forEach(field ->
                binaryObjectBuilder.setField(field.getName(), extractValueFrom(field)));
        return binaryObjectBuilder.build();
    }

    /**
     * Extracts value from field, can return null in case there is no value.
     * Any value can be null, that's why it is acceptable to return null here.
     *
     * @param field
     * @return extracted object or null if there is no value
     */
    private Object extractValueFrom(ICacheEntryValueField field) {
        Optional<Object> fieldValue = field.getFieldValue();

        if (isNoValueField(field)) {
            return null;
        }
        if (TypeUtils.isCollection(field.getTypeClassName())) {
            @SuppressWarnings("unchecked")
            List<ICacheEntryValueField> elements = (List<ICacheEntryValueField>) fieldValue.get();
            Collection<Object> collection = instantiate(field.getTypeClassName());
            elements.forEach(element -> collection.add(extractValueFrom(element)));
            return collection;
        }
        if (TypeUtils.isMap(field.getTypeClassName())) {
            @SuppressWarnings("unchecked")
            Map<ICacheEntryValueField, ICacheEntryValueField> dtoMap = (Map<ICacheEntryValueField, ICacheEntryValueField>) fieldValue.get();
            Map<Object, Object> map = instantiate(field.getTypeClassName());
            dtoMap.forEach((key, val) -> map.put(extractValueFrom(key), extractValueFrom(val)));
            return map;
        }
        if (field.hasNested()) {
            BinaryObjectBuilder nestedBuilder = ignite.binary().builder(field.getTypeClassName());
            Collection<ICacheEntryValueField> nestedFields = field.getNested();
            nestedFields.forEach(nestedField ->
                    nestedBuilder.setField(nestedField.getName(), extractValueFrom(nestedField)));
            return nestedBuilder.build();
        }

        return fieldValue.get();
    }

    private boolean isNoValueField(ICacheEntryValueField field) {
        Optional<Object> fieldVal = field.getFieldValue();
        return !fieldVal.isPresent() && !field.hasNested()
                || TypeUtils.isMap(field.getTypeClassName()) && !fieldVal.isPresent()
                || TypeUtils.isCollection(field.getTypeClassName()) && !fieldVal.isPresent();
    }

    private <T> T instantiate(String className) {
        try {
            Class<T> clazz = (Class<T>) Class.forName(className);
            Constructor<?> constructor = clazz.getDeclaredConstructor();
            return (T) constructor.newInstance();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private List<ICacheEntryValueField> parseBinaryObject(BinaryObject binaryObject, Map<String, IFieldInfo> binaryObjectFieldsInfo) {
        List<ICacheEntryValueField> cacheValueFieldList = new ArrayList<>();
        for (String fieldName : binaryObjectFieldsInfo.keySet()) {
            IFieldInfo fieldInfo = binaryObjectFieldsInfo.get(fieldName);

            CacheEntryValueField valueField = parseField(binaryObject.field(fieldInfo.getName()), fieldInfo);

            cacheValueFieldList.add(valueField);
        }

        return cacheValueFieldList;
    }

    private CacheEntryValueField parseField(Object fieldObj, IFieldInfo fieldInfo) {
        String fieldValueName = fieldInfo.getName();
        String fieldValueType = fieldInfo.getTypeInfo();
        Map<String, IFieldInfo> nestedFieldInfo = fieldInfo.getNested();
        if (nestedFieldInfo.isEmpty()) {
            return new CacheEntryValueField.Builder()
                    .setName(fieldValueName)
                    .setTypeClassName(fieldValueType)
                    .setValue(fieldInfo.getFieldDataConverter().convert(fieldObj))
                    .build();
        }

        CacheEntryValueField.Builder valueFieldBuilder = new CacheEntryValueField.Builder()
                .setName(fieldValueName)
                .setTypeClassName(fieldValueType);
        if (TypeUtils.isBinaryObject(fieldObj)) {
            valueFieldBuilder.setNested(parseBinaryObject((BinaryObject) fieldObj, nestedFieldInfo));
        }
        if (TypeUtils.isCollection(fieldValueType)) {
            List<ICacheEntryValueField> list = new ArrayList<>();
            Collection<?> valueCollection = (Collection<?>) fieldObj;
            valueCollection.forEach(element ->
                    list.add(parseField(element, fieldInfo.getNested().get(TypeUtils.VALUE))));
            valueFieldBuilder.setValue(list);
        }
        if (TypeUtils.isMap(fieldValueType)) {
            Map<ICacheEntryValueField, ICacheEntryValueField> map = new HashMap<>();
            Map<?, ?> valueMap = (Map<?, ?>) fieldObj;
            valueMap.forEach((key, val) ->
                    map.put(parseField(key, fieldInfo.getNested().get(TypeUtils.KEY)),
                            parseField(val, fieldInfo.getNested().get(TypeUtils.VALUE))));
            valueFieldBuilder.setValue(map);
        }

        return valueFieldBuilder.build();
    }

}
