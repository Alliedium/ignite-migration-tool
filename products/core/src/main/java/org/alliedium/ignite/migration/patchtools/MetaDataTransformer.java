package org.alliedium.ignite.migration.patchtools;

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

public class MetaDataTransformer implements IMetaTransformer<ICacheMetaData> {

    private static final IgniteObjectStringConverter converter = new IgniteObjectStringConverter();
    private final ICacheMetaData cacheMetaData;

    public MetaDataTransformer(ICacheMetaData cacheMetaData) {
        this.cacheMetaData = cacheMetaData;
    }

    @Override
    public MetaDataTransformer addFieldType(String name, Class<?> clazz) {
        // todo: add check for class, because not all classes are supported
        CacheConfiguration<Object, BinaryObject> cacheConfiguration =
                (CacheConfiguration<Object, BinaryObject>) converter.convertFromDto(cacheMetaData.getConfiguration().toString());
        String cacheEntryMeta = cacheMetaData.getEntryMeta().toString();
        Collection<QueryEntity> queryEntities = (Collection<QueryEntity>) converter.convertFromDto(cacheEntryMeta);

        for (QueryEntity queryEntity : queryEntities) {
            LinkedHashMap<String, String> fields = queryEntity.getFields();
            fields.put(name, clazz.getName());
            queryEntity.setFields(fields);
            Map<String, String> aliases = queryEntity.getAliases();
            aliases.put(name, name.toUpperCase());
            queryEntity.setAliases(aliases);
        }

        cacheConfiguration.setQueryEntities(queryEntities);

        // save modified data
        IDTOBuilder<ICacheEntryMetaData> cacheEntryMetaBuilder = new EntryMetaBuilder(queryEntities, converter);
        IDTOBuilder<ICacheConfigurationData> cacheConfigurationBuilder = new CacheConfigBuilder(cacheConfiguration, converter);

        return new MetaDataTransformer(new CacheMetaData(cacheMetaData.getName(), cacheConfigurationBuilder.build(),
                cacheEntryMetaBuilder.build()));
    }

    @Override
    public ICacheMetaData build() {
        return cacheMetaData;
    }
}
