package org.alliedium.ignite.migration.dao.converters;

import org.alliedium.ignite.migration.dao.datamanager.IIgniteCacheFieldMeta;
import org.alliedium.ignite.migration.dto.CacheEntryValue;
import org.alliedium.ignite.migration.dto.CacheEntryValueField;
import org.alliedium.ignite.migration.dto.CacheEntryValueFieldValue;
import org.alliedium.ignite.migration.dto.ICacheEntryValue;
import org.alliedium.ignite.migration.dto.ICacheEntryValueField;
import org.alliedium.ignite.migration.dto.ICacheEntryValueFieldValue;

import java.util.*;

import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;

/**
 * Unit executes conversion from an Apache Ignite {@link BinaryObject} to {@link ICacheEntryValue} and vice-versa.
 * When converting from {@link BinaryObject}, unit needs to be provided with meta information for each BinaryObject field
 * (as a list of {@link IIgniteCacheFieldMeta}) on initialization.
 * When converting from {@link ICacheEntryValue}, unit needs to be provided with correspondent Apache Ignite connection
 * and value type of an Apache Ignite cache, which BinaryObject is being built for.
 */
public class IgniteBinaryObjectConverter implements IIgniteDTOConverter<ICacheEntryValue, BinaryObject> {

    private Map<String, IIgniteCacheFieldMeta> cacheFieldMetaNameToSubjectMap;
    private Ignite ignite;
    private String cacheValueType;

    public IgniteBinaryObjectConverter(Map<String, IIgniteCacheFieldMeta> cacheFieldsMetaData) {
        this.cacheFieldMetaNameToSubjectMap = new HashMap<>(cacheFieldsMetaData);
    }

    public IgniteBinaryObjectConverter(Ignite ignite, String cacheValueType) {
        this.ignite = ignite;
        this.cacheValueType = cacheValueType;
    }

    public ICacheEntryValue convertFromEntity(BinaryObject binaryObject) {
        return new CacheEntryValue(parseBinaryObject(binaryObject));
    }

    public BinaryObject convertFromDto(ICacheEntryValue dto) {
        BinaryObjectBuilder binaryObjectBuilder = ignite.binary().builder(cacheValueType);
        for (String entryValueFieldName : dto.getFieldNamesList()) {
            Optional<Object> fieldValueValue = dto.getField(entryValueFieldName).getFieldValue().getValue();
            if (fieldValueValue.isPresent()) {
                binaryObjectBuilder.setField(entryValueFieldName, fieldValueValue.get());
            }
            else {
                binaryObjectBuilder.setField(entryValueFieldName, null);
            }
        }
        return binaryObjectBuilder.build();
    }

    private List<ICacheEntryValueField> parseBinaryObject(BinaryObject binaryObject) {
        List<ICacheEntryValueField> cacheValueFieldList = new ArrayList<>();
        for (String objectField : binaryObject.type().fieldNames()) {
            IIgniteCacheFieldMeta cacheFieldMeta = getFieldMeta(objectField);

            String fieldValueName = cacheFieldMeta.getName();
            String fieldValueType = cacheFieldMeta.getTypeInfo();
            ICacheEntryValueFieldValue valueFieldValue = new CacheEntryValueFieldValue(cacheFieldMeta.getFieldDataConverter().convert(binaryObject.field(objectField)));
            cacheValueFieldList.add(new CacheEntryValueField(fieldValueName, fieldValueType, valueFieldValue));
        }

        return cacheValueFieldList;
    }

    private IIgniteCacheFieldMeta getFieldMeta(String objectField) {
        return this.cacheFieldMetaNameToSubjectMap.get(objectField);
    }

}
