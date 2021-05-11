package org.alliedium.ignite.migration.dao.dtobuilder;

import org.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import org.alliedium.ignite.migration.dto.CacheConfigurationData;
import org.alliedium.ignite.migration.dto.ICacheConfigurationData;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Unit used for {@link ICacheConfigurationData} creation.
 * Configurations of separate Apache Ignite cache and correspondent {@link IIgniteDTOConverter} converter need to be provided on initialization.
 */
public class CacheConfigBuilder implements IDTOBuilder<ICacheConfigurationData> {

    private final CacheConfiguration<?, ?> igniteCacheConfig;

    private final IIgniteDTOConverter<String, Object> igniteDTOConverter;

    public CacheConfigBuilder(CacheConfiguration<?, ?> igniteCacheConfig, IIgniteDTOConverter<String, Object> igniteDTOConverter) {
        this.igniteCacheConfig = igniteCacheConfig;
        this.igniteDTOConverter = igniteDTOConverter;
    }


    public ICacheConfigurationData build() {
        String cacheConfiguration = igniteDTOConverter.convertFromEntity(igniteCacheConfig);
        return new CacheConfigurationData(cacheConfiguration);
    }

}