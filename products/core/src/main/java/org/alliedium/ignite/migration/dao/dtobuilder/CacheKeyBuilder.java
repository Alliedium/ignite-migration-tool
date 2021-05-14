package org.alliedium.ignite.migration.dao.dtobuilder;

import org.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import org.alliedium.ignite.migration.dto.CacheEntryKey;
import org.alliedium.ignite.migration.dto.ICacheEntryKey;
import org.apache.ignite.binary.BinaryObject;

/**
 * Unit used for {@link ICacheEntryKey} creation.
 * Apache Ignite cache entry key and correspondent realization of {@link IIgniteDTOConverter} converter need to be provided on initialization.
 */
public class CacheKeyBuilder implements IDTOBuilder<ICacheEntryKey> {

    private final Object igniteCacheKey;

    private final IIgniteDTOConverter<String, Object> igniteDTOConverter;

    public CacheKeyBuilder(Object igniteCacheKey, IIgniteDTOConverter<String, Object> igniteDTOConverter) {
        this.igniteCacheKey = igniteCacheKey;
        this.igniteDTOConverter = igniteDTOConverter;
    }


    public ICacheEntryKey build() {
        String cacheKeyXMLString;
        if (igniteCacheKey instanceof BinaryObject) {
            BinaryObject binObjKey = (BinaryObject) igniteCacheKey;
            cacheKeyXMLString = igniteDTOConverter.convertFromEntity(binObjKey.deserialize());
        }
        else {
            cacheKeyXMLString = igniteDTOConverter.convertFromEntity(igniteCacheKey);
        }

        return new CacheEntryKey(cacheKeyXMLString);
    }

}
