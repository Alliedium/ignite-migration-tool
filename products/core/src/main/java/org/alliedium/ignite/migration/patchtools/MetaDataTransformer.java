package org.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import org.alliedium.ignite.migration.dao.converters.IgniteObjectStringConverter;
import org.alliedium.ignite.migration.dao.dtobuilder.CacheConfigBuilder;
import org.alliedium.ignite.migration.dao.dtobuilder.EntryMetaBuilder;
import org.alliedium.ignite.migration.dao.dtobuilder.IDTOBuilder;
import org.alliedium.ignite.migration.dto.CacheMetaData;
import org.alliedium.ignite.migration.dto.ICacheConfigurationData;
import org.alliedium.ignite.migration.dto.ICacheEntryMetaData;
import org.alliedium.ignite.migration.dto.ICacheMetaData;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

public class MetaDataTransformer implements IMetaTransformer<ICacheMetaData> {

    private static final IIgniteDTOConverter<String, Collection<QueryEntity>> queryEntityConverter = IgniteObjectStringConverter.QUERY_ENTITY_CONVERTER;
    private static final IIgniteDTOConverter<String, CacheConfiguration<Object, BinaryObject>> cacheConfigConverter = IgniteObjectStringConverter.CACHE_CONFIG_CONVERTER;
    private static final IIgniteDTOConverter<String, Object> converter = IgniteObjectStringConverter.GENERIC_CONVERTER;
    private final ICacheMetaData cacheMetaData;

    public MetaDataTransformer(ICacheMetaData cacheMetaData) {
        this.cacheMetaData = cacheMetaData;
    }

    @Override
    public MetaDataTransformer addFieldType(String name, Class<?> clazz) {
        return modifyMetaData(queryEntity -> {
            LinkedHashMap<String, String> fields = queryEntity.getFields();
            fields.put(name, clazz.getName());
            queryEntity.setFields(fields);
            Map<String, String> aliases = queryEntity.getAliases();
            aliases.put(name, name.toUpperCase());
            queryEntity.setAliases(aliases);
        });
    }

    @Override
    public MetaDataTransformer removeFieldType(String name) {
        return modifyMetaData(queryEntity -> {
            LinkedHashMap<String, String> fields = queryEntity.getFields();
            fields.remove(name);
            queryEntity.setFields(fields);
            Map<String, String> aliases = queryEntity.getAliases();
            aliases.remove(name);
            queryEntity.setAliases(aliases);
        });
    }

    @Override
    public ICacheMetaData build() {
        return cacheMetaData;
    }

    private MetaDataTransformer modifyMetaData(Consumer<QueryEntity> queryEntityModifier) {
        // todo: add check for class, because not all classes are supported
        CacheConfiguration<Object, BinaryObject> cacheConfiguration =
                cacheConfigConverter.convertFromDTO(cacheMetaData.getConfiguration().toString());
        String cacheEntryMeta = cacheMetaData.getEntryMeta().toString();
        Collection<QueryEntity> queryEntities = queryEntityConverter.convertFromDTO(cacheEntryMeta);

        queryEntities.forEach(queryEntityModifier);

        cacheConfiguration.setQueryEntities(queryEntities);

        // save modified data
        IDTOBuilder<ICacheEntryMetaData> cacheEntryMetaBuilder = new EntryMetaBuilder(queryEntities, converter);
        IDTOBuilder<ICacheConfigurationData> cacheConfigurationBuilder = new CacheConfigBuilder(cacheConfiguration, converter);

        return new MetaDataTransformer(new CacheMetaData(cacheMetaData.getCacheName(), cacheConfigurationBuilder.build(),
                cacheEntryMetaBuilder.build()));
    }
}
