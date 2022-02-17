package io.github.alliedium.ignite.migration.dao;

import io.github.alliedium.ignite.migration.IDataWriter;
import io.github.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import io.github.alliedium.ignite.migration.dao.converters.IgniteObjectStringConverter;
import io.github.alliedium.ignite.migration.dto.ICacheMetaData;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import java.util.Collection;

public class IgniteCacheMetaDataWriter extends IgniteDataWriter implements IDataWriter<ICacheMetaData> {

    private final IIgniteDTOConverter<String, CacheConfiguration<Object, BinaryObject>> configurationConverter = IgniteObjectStringConverter.CACHE_CONFIG_CONVERTER;
    private final IIgniteDTOConverter<String, Collection<QueryEntity>> queryEntityConverter = IgniteObjectStringConverter.QUERY_ENTITY_CONVERTER;

    public IgniteCacheMetaDataWriter(IIgniteDTOConverter<String, Object> cacheKeyConverter, Ignite ignite) {
        super(cacheKeyConverter, ignite);
    }

    @Override
    public void write(ICacheMetaData data) {
        String recreatingCacheName = data.getCacheName();
        if (cacheNeedsToBeRestored(recreatingCacheName) && ignite.cacheNames().contains(recreatingCacheName)) {
            logger.info("Skipping cache recreation for already existing one: " + recreatingCacheName);
        } else {
            recreateIgniteCache(ignite, data);
        }
    }

    private void recreateIgniteCache(Ignite ignite, ICacheMetaData cacheMetaData) {
        String cacheConfigurationDTO = cacheMetaData.getConfiguration().toString();
        CacheConfiguration<Object, BinaryObject> recreatingCacheConfiguration = configurationConverter.convertFromDTO(cacheConfigurationDTO);
        String cacheEntryMeta = cacheMetaData.getEntryMeta().toString();
        Collection<QueryEntity> recreatingCacheQueryEntities = queryEntityConverter.convertFromDTO(cacheEntryMeta);

        recreatingCacheConfiguration.setQueryEntities(recreatingCacheQueryEntities);

        logger.info("Creating ignite cache: " + cacheMetaData.getCacheName());
        ignite.getOrCreateCache(recreatingCacheConfiguration);
        logger.info(String.format("Cache created [%s] successfully", cacheMetaData.getCacheName()));
    }
}
