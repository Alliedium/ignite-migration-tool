package org.alliedium.ignite.migration.dao.datamanager;

import org.alliedium.ignite.migration.Utils;
import org.alliedium.ignite.migration.dao.converters.IIgniteBinaryDataConverter;
import org.alliedium.ignite.migration.dao.converters.IgniteEnumDataConverter;
import org.alliedium.ignite.migration.dao.converters.IgniteGenericDataConverter;

import java.util.*;

import org.alliedium.ignite.migration.dao.converters.TypesResolver;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;

/**
 * Unit responsible for figuring out the meta-data of ignite cache fields (field name, value type class name,
 * converter needed to get a common Java representation when the field value is of Ignite specific class).
 * Meta-data required for ignite cache fields serialization is being defined from {@link BinaryObject} and {@link org.apache.ignite.cache.QueryEntity}.
 * Resulting output is represented as a list of {@link IgniteCacheFieldMeta}.
 */
public class IgniteCacheFieldMetaBuilder implements IIgniteCacheFieldMetaBuilder {

    private final BinaryObject binaryObject;
    private final Collection<QueryEntity> igniteCacheQueryEntities;

    public IgniteCacheFieldMetaBuilder(BinaryObject binaryObject, Collection<QueryEntity> igniteCacheQueryEntities) {
        this.binaryObject = binaryObject;
        this.igniteCacheQueryEntities = igniteCacheQueryEntities;
    }

    @Override
    public Map<String, IIgniteCacheFieldMeta> getFieldsMetaData() {
        Map<String, IIgniteCacheFieldMeta> cacheFieldsMetaData = new HashMap<>();
        Map<String, String> queryEntityFieldsInfo = getQueryEntityFieldsInfo();

        for (String fieldName : binaryObject.type().fieldNames()) {
            String typeInfoFromQueryEntity = Objects.requireNonNull(queryEntityFieldsInfo.get(fieldName.toUpperCase()),
                    String.format("Query entity for field [%s] was not found", fieldName));
            String typeInfo = TypesResolver.toAvroType(typeInfoFromQueryEntity);
            IIgniteBinaryDataConverter fieldDataConverter;

            if (binaryObject.field(fieldName) != null &&
                    BinaryEnumObjectImpl.class.isAssignableFrom(binaryObject.field(fieldName).getClass())) {
                fieldDataConverter = new IgniteEnumDataConverter();
            } else {
                fieldDataConverter = new IgniteGenericDataConverter();
            }

            cacheFieldsMetaData.put(fieldName, new IgniteCacheFieldMeta(fieldName, typeInfo, fieldDataConverter));
        }

        return cacheFieldsMetaData;
    }

    private Map<String, String> getQueryEntityFieldsInfo() {
        if (igniteCacheQueryEntities.isEmpty()) {
            throw new IllegalArgumentException("ignite migration tool does not work without ignite cache query entities");
        }

        QueryEntity singleQueryEntity = igniteCacheQueryEntities.iterator().next();
        return Utils.capitalizeMapKeys(singleQueryEntity.getFields());
    }

}
