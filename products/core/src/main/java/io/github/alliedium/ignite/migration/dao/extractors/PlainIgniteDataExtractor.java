package io.github.alliedium.ignite.migration.dao.extractors;

import io.github.alliedium.ignite.migration.IDispatcher;
import io.github.alliedium.ignite.migration.dao.converters.PlainEntryValueWrapper;
import io.github.alliedium.ignite.migration.dao.dataaccessor.IIgniteCacheDAO;
import io.github.alliedium.ignite.migration.dto.CacheData;
import io.github.alliedium.ignite.migration.dto.ICacheData;
import io.github.alliedium.ignite.migration.dto.ICacheEntryValue;
import io.github.alliedium.ignite.migration.serializer.utils.FieldNames;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;

class PlainIgniteDataExtractor extends AbstractIgniteDataExtractor {

    PlainIgniteDataExtractor(String cacheName, IIgniteCacheDAO igniteCacheDAO, IDispatcher<ICacheData> cacheDataDispatcher) {
        super(cacheName, igniteCacheDAO, cacheDataDispatcher);
    }

    @Override
    public void extract() {
        ScanQuery<Object, BinaryObject> scanQuery = new ScanQuery<>();
        igniteCacheDAO.getBinaryCache().query(scanQuery).forEach(entry -> {
            ICacheEntryValue key = PlainEntryValueWrapper.wrap(entry.getKey(), FieldNames.KEY_FIELD_NAME);
            ICacheEntryValue val = cacheValueConverter.convertFromEntity(entry.getValue());
            cacheDataDispatcher.publish(new CacheData(cacheName, key, val));
        });
    }
}
