package org.alliedium.ignite.migration.dao;

import org.alliedium.ignite.migration.IDispatcher;
import org.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import org.alliedium.ignite.migration.dao.converters.IgniteBinaryObjectConverter;
import org.alliedium.ignite.migration.dao.converters.IgniteObjectStringConverter;
import org.alliedium.ignite.migration.dao.dataaccessor.IIgniteCacheDAO;
import org.alliedium.ignite.migration.dao.dataaccessor.IIgniteDAO;
import org.alliedium.ignite.migration.dao.dataaccessor.IgniteAtomicLongNamesProvider;
import org.alliedium.ignite.migration.dao.datamanager.IIgniteCacheFieldMetaBuilder;
import org.alliedium.ignite.migration.dao.datamanager.IgniteCacheFieldMetaBuilder;
import org.alliedium.ignite.migration.dao.dtobuilder.CacheConfigBuilder;
import org.alliedium.ignite.migration.dao.dtobuilder.CacheKeyBuilder;
import org.alliedium.ignite.migration.dao.dtobuilder.EntryMetaBuilder;
import org.alliedium.ignite.migration.dao.dtobuilder.IDTOBuilder;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.alliedium.ignite.migration.dto.*;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IgniteScanner is a unit responsible for getting the Ignite meta-data and stored data, processing this data and returning as an ignite specific-free DTO.
 * In terms of Ignite data processing all the Ignite specific links are being removed. Data is being converted to common Java formats for further flexibility.
 * Each separate ignite cache is being converted to DTO elements {@link ICacheData}.
 */
public class IgniteScanner implements IIgniteReader {

    private static final Logger logger = LoggerFactory.getLogger(IgniteScanner.class);

    private final IgniteAtomicLongNamesProvider atomicNamesProvider;
    private final Ignite ignite;
    private final IIgniteDTOConverter<String, Object> converter;

    public IgniteScanner(IgniteAtomicLongNamesProvider atomicNamesProvider, Ignite ignite) {
        this.atomicNamesProvider = atomicNamesProvider;
        this.ignite = ignite;
        converter = new IgniteObjectStringConverter();
    }

    @Override
    public void read(IIgniteDAO igniteDAO, IDispatcher<ICacheMetaData> cacheMetaDataDispatcher,
                     IDispatcher<ICacheData> cacheDataDispatcher, IDispatcher<Map.Entry<String, Long>> atomicLongsDispatcher) {
        try {
            igniteDAO.getCacheNames().forEach(igniteCacheName -> {
                logger.info("Starting cache processing of " + igniteCacheName);
                convertCacheToDTO(igniteDAO, igniteCacheName, cacheMetaDataDispatcher, cacheDataDispatcher);
                logger.info("Processed cache " + igniteCacheName);
            });


            atomicNamesProvider.getAtomicNames().forEach(atomicName -> {
                IgniteAtomicLong atomicLong = ignite.atomicLong(atomicName, 0, false);
                if (atomicLong == null) {
                    logger.info("atomic long with name: " + atomicName + " was not found");
                    return;
                }

                atomicLongsDispatcher.publish(new AbstractMap.SimpleEntry<>(atomicName, atomicLong.get()));
            });
        } finally {
            cacheMetaDataDispatcher.finish();
            cacheDataDispatcher.finish();
            atomicLongsDispatcher.finish();
        }
    }

    private void convertCacheToDTO(IIgniteDAO igniteDAO, String cacheName, IDispatcher<ICacheMetaData> cacheMetaDataDispatcher,
                                   IDispatcher<ICacheData> cacheDataDispatcher) {
        IIgniteCacheDAO igniteCacheDAO = igniteDAO.getIgniteCacheDAO(cacheName);

        ICacheConfigurationData cacheConfigurationDTO = getCacheConfigDTO(igniteCacheDAO);
        ICacheEntryMetaData cacheEntryMetaDTO = getCacheEntryMetaDTO(igniteCacheDAO);
        ICacheMetaData cacheMetaData = new CacheMetaData(cacheName, cacheConfigurationDTO, cacheEntryMetaDTO);

        cacheMetaDataDispatcher.publish(cacheMetaData);

        if (igniteDAO.cacheIsEmpty(cacheName)) {
            logger.info(cacheName + " cache is empty. Nothing to convert to DTO");
            return;
        }

        getCacheDataDTO(cacheName, igniteCacheDAO, cacheDataDispatcher);
    }

    private ICacheConfigurationData getCacheConfigDTO(IIgniteCacheDAO igniteCacheDAO) {
        IDTOBuilder<ICacheConfigurationData> cacheConfigurationDTOBuilder = new CacheConfigBuilder(igniteCacheDAO.getCacheConfiguration(), converter);
        return cacheConfigurationDTOBuilder.build();
    }

    private ICacheEntryMetaData getCacheEntryMetaDTO(IIgniteCacheDAO igniteCacheDAO) {
        IDTOBuilder<ICacheEntryMetaData> cacheEntryMetaDTOBuilder = new EntryMetaBuilder(igniteCacheDAO.getCacheQueryEntities(), converter);
        return cacheEntryMetaDTOBuilder.build();
    }

    private void getCacheDataDTO(String cacheName, IIgniteCacheDAO igniteCacheDAO, IDispatcher<ICacheData> cacheDataDispatcher) {
        BinaryObject cacheBinaryObject = igniteCacheDAO.getAnyValue();
        IIgniteCacheFieldMetaBuilder cacheFieldMetaBuilder = new IgniteCacheFieldMetaBuilder(cacheBinaryObject, igniteCacheDAO.getCacheQueryEntities());
        IIgniteDTOConverter<ICacheEntryValue, BinaryObject> cacheValueConverter = new IgniteBinaryObjectConverter(cacheFieldMetaBuilder.getFieldsMetaData());

        ScanQuery<Object, BinaryObject> scanQuery = new ScanQuery<>();

        igniteCacheDAO.getBinaryCache().withKeepBinary().query(scanQuery).forEach(entry -> {
            ICacheEntryKey cacheKeyDTO = new CacheKeyBuilder(entry.getKey(), converter).build();
            BinaryObject binaryObject = entry.getValue();
            ICacheEntryValue cacheValueDTO = cacheValueConverter.convertFromEntity(binaryObject);

            cacheDataDispatcher.publish(new CacheData(cacheName, cacheKeyDTO, cacheValueDTO));
        });
    }
}
