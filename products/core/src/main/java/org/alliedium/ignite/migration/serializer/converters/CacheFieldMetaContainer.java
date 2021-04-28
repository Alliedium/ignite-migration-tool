package org.alliedium.ignite.migration.serializer.converters;

import org.alliedium.ignite.migration.dto.ICacheEntryValue;
import org.alliedium.ignite.migration.dto.ICacheEntryValueField;
import org.alliedium.ignite.migration.serializer.converters.datatypes.AvroDerivedTypeConverterFactory;
import org.alliedium.ignite.migration.serializer.converters.datatypes.IAvroDerivedTypeConverter;
import org.alliedium.ignite.migration.serializer.converters.schemafields.IAvroSchemaFieldAssembler;
import org.alliedium.ignite.migration.serializer.converters.schemafields.SchemaFieldAssemblerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Unit contains meta-data as {@link ICacheFieldMeta} for each separate {@link ICacheEntryValueField}.
 * Required {@link ICacheFieldMeta} can be requested by by field name ({@link ICacheEntryValueField#getName()}).
 * Needs to be created in terms of serializing DTO to avro.
 */
public class CacheFieldMetaContainer implements ICacheFieldMetaContainer {

    private final Map<String, ICacheFieldMeta> fieldTypeConverterContainersMap;

    public CacheFieldMetaContainer(ICacheEntryValue cacheEntryValue) {
        List<ICacheFieldMeta> cacheFieldAvroMetaList = new ArrayList<>();

        List<String> cacheValueFieldNameList = cacheEntryValue.getFieldNamesList();
        //TODO: to add a mechanism which checks whether provided DTO can be serialized to avro (all the incoming value types are supported or custom converters are available)
        for (String cacheValueFieldName : cacheValueFieldNameList) {
            String cacheValueFieldTypeClassName = cacheEntryValue.getField(cacheValueFieldName).getTypeClassName();
            IAvroSchemaFieldAssembler avroSchemaFieldAssembler = SchemaFieldAssemblerFactory.get(cacheValueFieldTypeClassName);
            IAvroDerivedTypeConverter avroDerivedTypeConverter = AvroDerivedTypeConverterFactory.get(cacheValueFieldTypeClassName);
            ICacheFieldMeta cacheFieldAvroMeta = new CacheFieldMeta(cacheValueFieldName, avroSchemaFieldAssembler, avroDerivedTypeConverter);
            cacheFieldAvroMetaList.add(cacheFieldAvroMeta);
        }

        this.fieldTypeConverterContainersMap = cacheFieldAvroMetaList.stream().collect(Collectors.toMap(ICacheFieldMeta::getName, ICacheFieldMeta -> ICacheFieldMeta, (u, v) -> {
            throw new IllegalStateException(String.format("Duplicate fieldName %s", u));
        }, HashMap::new));
    }

    public ICacheFieldMeta getFieldTypeMeta(String fieldName) {
        return this.fieldTypeConverterContainersMap.get(fieldName);
    }
}
