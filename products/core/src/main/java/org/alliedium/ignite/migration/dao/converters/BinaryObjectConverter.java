package org.alliedium.ignite.migration.dao.converters;

import org.alliedium.ignite.migration.dao.datamanager.IFieldInfo;
import org.alliedium.ignite.migration.dto.CacheEntryValue;
import org.alliedium.ignite.migration.dto.CacheEntryValueField;
import org.alliedium.ignite.migration.dto.ICacheEntryValue;
import org.alliedium.ignite.migration.dto.ICacheEntryValueField;

import java.util.*;

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
        dto.getFields().forEach(field -> setField(binaryObjectBuilder, field));
        return binaryObjectBuilder.build();
    }

    private void setField(BinaryObjectBuilder builder, ICacheEntryValueField field) {
        Optional<Object> fieldValue = field.getFieldValue();
        if (!field.hasNested()) {
            if (fieldValue.isPresent()) {
                builder.setField(field.getName(), fieldValue.get());
            } else {
                builder.setField(field.getName(), null);
            }
            return;
        }

        BinaryObjectBuilder nestedBuilder = ignite.binary().builder(field.getTypeClassName());
        Collection<ICacheEntryValueField> nestedFields = field.getNested();
        nestedFields.forEach(nestedField -> setField(nestedBuilder, nestedField));
        builder.setField(field.getName(), nestedBuilder.build());
    }

    private List<ICacheEntryValueField> parseBinaryObject(BinaryObject binaryObject, Map<String, IFieldInfo> binaryObjectFieldsInfo) {
        List<ICacheEntryValueField> cacheValueFieldList = new ArrayList<>();
        for (String fieldName : binaryObjectFieldsInfo.keySet()) {
            IFieldInfo fieldInfo = binaryObjectFieldsInfo.get(fieldName);

            String fieldValueName = fieldInfo.getName();
            String fieldValueType = fieldInfo.getTypeInfo();
            CacheEntryValueField valueField;
            Map<String, IFieldInfo> nestedFieldInfo = fieldInfo.getNested();
            if (nestedFieldInfo.isEmpty()) {
                valueField = new CacheEntryValueField.Builder()
                        .setName(fieldValueName)
                        .setTypeClassName(fieldValueType)
                        .setValue(fieldInfo.getFieldDataConverter().convert(binaryObject.field(fieldName)))
                        .build();
            } else {
                valueField = new CacheEntryValueField.Builder()
                        .setName(fieldValueName)
                        .setTypeClassName(fieldValueType)
                        .setNested(parseBinaryObject(binaryObject.field(fieldInfo.getName()), nestedFieldInfo))
                        .build();
            }
            cacheValueFieldList.add(valueField);
        }

        return cacheValueFieldList;
    }

}
