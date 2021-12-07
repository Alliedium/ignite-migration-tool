package org.alliedium.ignite.migration.dao;

import org.alliedium.ignite.migration.IDispatcher;
import org.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import org.alliedium.ignite.migration.dao.converters.IgniteObjectStringConverter;
import org.alliedium.ignite.migration.dao.dataaccessor.IIgniteCacheDAO;
import org.alliedium.ignite.migration.dao.dataaccessor.IIgniteDAO;
import org.alliedium.ignite.migration.IgniteAtomicLongNamesProvider;
import org.alliedium.ignite.migration.dao.dtobuilder.CacheConfigBuilder;
import org.alliedium.ignite.migration.dao.dtobuilder.EntryMetaBuilder;
import org.alliedium.ignite.migration.dao.dtobuilder.IDTOBuilder;

import java.util.*;

import org.alliedium.ignite.migration.dao.extractors.IgniteDataExtractor;
import org.alliedium.ignite.migration.dao.extractors.IgniteDataExtractorFactory;
import org.alliedium.ignite.migration.dto.*;
import org.alliedium.ignite.migration.util.BinaryObjectUtil;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.binary.BinaryObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IgniteScanner is a unit responsible for getting the Apache Ignite meta-data and stored data, processing this data and returning as an Apache Ignite specific-free DTO.
 * In terms of Apache Ignite data processing all the Apache Ignite specific links are being removed. Data is being converted to common Java formats for further flexibility.
 * Each separate Apache Ignite cache is being converted to DTO elements {@link ICacheData}.
 */
public class IgniteScanner implements IIgniteReader {

    private static final Logger logger = LoggerFactory.getLogger(IgniteScanner.class);

    private final IgniteAtomicLongNamesProvider atomicNamesProvider;
    private final Ignite ignite;
    private final IIgniteDTOConverter<String, Object> converter;

    public IgniteScanner(IgniteAtomicLongNamesProvider atomicNamesProvider, Ignite ignite) {
        this.atomicNamesProvider = atomicNamesProvider;
        this.ignite = ignite;
        converter = IgniteObjectStringConverter.GENERIC_CONVERTER;
    }

    @Override
    public void read(IIgniteDAO igniteDAO, IDispatcher<ICacheMetaData> cacheMetaDataDispatcher,
                     IDispatcher<ICacheData> cacheDataDispatcher, IDispatcher<Map.Entry<String, Long>> atomicLongsDispatcher) {
        try {
            igniteDAO.getCacheNames().forEach(igniteCacheName -> {
               try {
                   logger.info("Starting cache processing of " + igniteCacheName);
                   convertCacheToDTO(igniteDAO, igniteCacheName, cacheMetaDataDispatcher, cacheDataDispatcher);
                   logger.info("Processed cache " + igniteCacheName);
               } catch (Exception e) {
                   throw new IllegalStateException(
                           String.format("cache name: %s, %s", igniteCacheName, e.getMessage()), e);
               }
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

        CacheDataTypes cacheDataTypes = extractCacheDataTypes(igniteCacheDAO);

        ICacheMetaData cacheMetaData = new CacheMetaData(
                cacheName, cacheConfigurationDTO, cacheEntryMetaDTO, cacheDataTypes);

        cacheMetaDataDispatcher.publish(cacheMetaData);

        if (igniteDAO.cacheIsEmpty(cacheName)) {
            logger.info(cacheName + " cache is empty. Nothing to convert to DTO");
            return;
        }

        IgniteDataExtractor extractor = IgniteDataExtractorFactory.create(cacheName, igniteCacheDAO, cacheDataDispatcher);
        extractor.extract();
    }

    private ICacheConfigurationData getCacheConfigDTO(IIgniteCacheDAO igniteCacheDAO) {
        IDTOBuilder<ICacheConfigurationData> cacheConfigurationDTOBuilder = new CacheConfigBuilder(igniteCacheDAO.getCacheConfiguration(), converter);
        return cacheConfigurationDTOBuilder.build();
    }

    private ICacheEntryMetaData getCacheEntryMetaDTO(IIgniteCacheDAO igniteCacheDAO) {
        IDTOBuilder<ICacheEntryMetaData> cacheEntryMetaDTOBuilder = new EntryMetaBuilder(igniteCacheDAO.getCacheQueryEntities(), converter);
        return cacheEntryMetaDTOBuilder.build();
    }

    /**
     * Extracts cache data types from cache key and val objects.
     * In case cache is empty data types are taken from query entities.
     * In case cache is empty and no query entities are set will return blank lines instead of types.
     * There should not be a situation when cache data types are blank lines but cache data exists
     * @param igniteCacheDAO
     * @return cache data types
     */
    private CacheDataTypes extractCacheDataTypes(IIgniteCacheDAO igniteCacheDAO) {
        if (igniteCacheDAO.isCacheEmpty()) {
            Optional<String> keyType = igniteCacheDAO.getCacheKeyType();
            Optional<String> valType = igniteCacheDAO.getCacheValueType();
            return new CacheDataTypes(keyType.orElse( ""), valType.orElse(""));
        }

        BinaryObject cacheBinaryObject = igniteCacheDAO.getAnyValue();
        String valType = BinaryObjectUtil.getBinaryTypeName(cacheBinaryObject);
        Object anyKey = igniteCacheDAO.getAnyKey();
        if (anyKey instanceof BinaryObject) {
            BinaryObject key = (BinaryObject) anyKey;
            return new CacheDataTypes(BinaryObjectUtil.getBinaryTypeName(key), valType);
        }

        return new CacheDataTypes(anyKey.getClass().getName(), valType);
    }
}
