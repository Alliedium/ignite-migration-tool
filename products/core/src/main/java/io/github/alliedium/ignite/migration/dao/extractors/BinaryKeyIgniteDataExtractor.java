package io.github.alliedium.ignite.migration.dao.extractors;

import io.github.alliedium.ignite.migration.IDispatcher;
import io.github.alliedium.ignite.migration.dao.converters.BinaryObjectConverter;
import io.github.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import io.github.alliedium.ignite.migration.dao.dataaccessor.IIgniteCacheDAO;
import io.github.alliedium.ignite.migration.dao.datamanager.BinaryObjectFieldsInfoResolver;
import io.github.alliedium.ignite.migration.dao.datamanager.IBinaryObjectFieldInfoResolver;
import io.github.alliedium.ignite.migration.dto.CacheData;
import io.github.alliedium.ignite.migration.dto.ICacheData;
import io.github.alliedium.ignite.migration.dto.ICacheEntryValue;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;

class BinaryKeyIgniteDataExtractor extends AbstractIgniteDataExtractor {

    private IIgniteDTOConverter<ICacheEntryValue, BinaryObject> keyConverter;

    BinaryKeyIgniteDataExtractor(String cacheName, IIgniteCacheDAO igniteCacheDAO, IDispatcher<ICacheData> cacheDataDispatcher) {
        super(cacheName, igniteCacheDAO, cacheDataDispatcher);
        Object anyKey = igniteCacheDAO.getAnyKey();
        IBinaryObjectFieldInfoResolver keyFieldsResolver = new BinaryObjectFieldsInfoResolver((BinaryObject) anyKey);
        keyConverter = new BinaryObjectConverter(keyFieldsResolver.resolveFieldsInfo());
    }

    @Override
    public void extract() {
        ScanQuery<BinaryObject, BinaryObject> scanQuery = new ScanQuery<>();
        igniteCacheDAO.getBinaryCache().withKeepBinary().query(scanQuery).forEach(entry -> {
            ICacheEntryValue key = keyConverter.convertFromEntity(entry.getKey());
            ICacheEntryValue val = cacheValueConverter.convertFromEntity(entry.getValue());
            cacheDataDispatcher.publish(new CacheData(cacheName, key, val));
        });
    }
}
