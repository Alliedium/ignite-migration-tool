package org.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.dao.converters.TypesResolver;
import org.alliedium.ignite.migration.dto.*;

import java.util.ArrayList;
import java.util.List;

public class CacheDataTransformer implements ITransformer<ICacheData> {

    private final ICacheData cacheData;

    public CacheDataTransformer(ICacheData cacheData) {
        this.cacheData = cacheData;
    }

    @Override
    public CacheDataTransformer addField(String name, Object val) {
        // todo: here could go types check, cause only basic types work
        List<ICacheEntryValueField> fields = cacheData.getCacheEntryValue().getFields();
        fields.add(new CacheEntryValueField.Builder()
                .setName(name)
                .setTypeClassName(val.getClass().getName())
                .setValue(val)
                .build());
        List<ICacheEntryValueField> resultFields = resolveFieldTypesForAvro(fields);

        return new CacheDataTransformer(new CacheData(cacheData.getCacheName(), cacheData.getCacheEntryKey(),
                new CacheEntryValue(resultFields)));
    }

    @Override
    public ITransformer<ICacheData> removeField(String name) {
        // todo: here could go types check, cause only basic types work
        List<ICacheEntryValueField> fields = cacheData.getCacheEntryValue().getFields();
        fields.removeIf(field -> field.getName().equals(name));
        List<ICacheEntryValueField> resultFields = resolveFieldTypesForAvro(fields);

        return new CacheDataTransformer(new CacheData(cacheData.getCacheName(), cacheData.getCacheEntryKey(),
                new CacheEntryValue(resultFields)));
    }

    @Override
    public ITransformer<ICacheData> convertFieldsToAvro() {
        List<ICacheEntryValueField> fields = cacheData.getCacheEntryValue().getFields();
        List<ICacheEntryValueField> resultFields = resolveFieldTypesForAvro(fields);
        return new CacheDataTransformer(new CacheData(cacheData.getCacheName(), cacheData.getCacheEntryKey(),
                new CacheEntryValue(resultFields)));
    }

    private List<ICacheEntryValueField> resolveFieldTypesForAvro(List<ICacheEntryValueField> fields) {
        List<ICacheEntryValueField> resultFields = new ArrayList<>();
        fields.forEach(field ->
                resultFields.add(new CacheEntryValueField.Builder()
                        .setName(field.getName())
                        .setTypeClassName(TypesResolver.toAvroType(field.getTypeClassName()))
                        .setValue(field.getFieldValue().get())
                        .build()));
        return resultFields;
    }

    public ICacheData build() {
        return cacheData;
    }
}
