package org.alliedium.ignite.migration.dao.dtobuilder;

import org.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import org.alliedium.ignite.migration.dto.CacheEntryMetaData;
import org.alliedium.ignite.migration.dto.ICacheEntryMetaData;
import java.util.Collection;

import org.apache.ignite.cache.QueryEntity;

/**
 * Unit used for {@link ICacheEntryMetaData} creation.
 * Query entities of an ignite cache {@link org.apache.ignite.IgniteCache#getConfiguration(Class)#igniteCacheQueryEntities} and
 * correspondent realization of {@link IIgniteDTOConverter} converter need to be provided on initialization.
 */
public class EntryMetaBuilder implements IDTOBuilder<ICacheEntryMetaData> {

    private final Collection<QueryEntity> igniteCacheQueryEntities;

    private final IIgniteDTOConverter<String, Object> igniteDTOConverter;

    public EntryMetaBuilder(Collection<QueryEntity> igniteCacheQueryEntities, IIgniteDTOConverter<String, Object> igniteDTOConverter) {
        this.igniteCacheQueryEntities = igniteCacheQueryEntities;
        this.igniteDTOConverter = igniteDTOConverter;
    }


    public ICacheEntryMetaData build() {
        String cacheConfiguration = igniteDTOConverter.convertFromEntity(igniteCacheQueryEntities);
        return new CacheEntryMetaData(cacheConfiguration);
    }
}
