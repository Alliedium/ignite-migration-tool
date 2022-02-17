package io.github.alliedium.ignite.migration.dao.extractors;

import io.github.alliedium.ignite.migration.IDispatcher;
import io.github.alliedium.ignite.migration.dao.converters.BinaryObjectConverter;
import io.github.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import io.github.alliedium.ignite.migration.dao.dataaccessor.IIgniteCacheDAO;
import io.github.alliedium.ignite.migration.dao.datamanager.BinaryObjectFieldsInfoResolver;
import io.github.alliedium.ignite.migration.dao.datamanager.IBinaryObjectFieldInfoResolver;
import io.github.alliedium.ignite.migration.dto.ICacheData;
import io.github.alliedium.ignite.migration.dto.ICacheEntryValue;
import org.apache.ignite.binary.BinaryObject;

abstract class AbstractIgniteDataExtractor implements IgniteDataExtractor {

    protected final String cacheName;
    protected final IIgniteCacheDAO igniteCacheDAO;
    protected final IDispatcher<ICacheData> cacheDataDispatcher;
    protected final IIgniteDTOConverter<ICacheEntryValue, BinaryObject> cacheValueConverter;

    AbstractIgniteDataExtractor(String cacheName, IIgniteCacheDAO igniteCacheDAO, IDispatcher<ICacheData> cacheDataDispatcher) {
        BinaryObject cacheBinaryObject = igniteCacheDAO.getAnyValue();
        IBinaryObjectFieldInfoResolver cacheFieldMetaBuilder = new BinaryObjectFieldsInfoResolver(cacheBinaryObject);
        cacheValueConverter = new BinaryObjectConverter(cacheFieldMetaBuilder.resolveFieldsInfo());
        this.cacheName = cacheName;
        this.igniteCacheDAO = igniteCacheDAO;
        this.cacheDataDispatcher = cacheDataDispatcher;
    }
}
