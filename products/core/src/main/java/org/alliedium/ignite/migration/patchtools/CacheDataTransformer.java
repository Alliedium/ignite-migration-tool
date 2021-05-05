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
        fields.add(new CacheEntryValueField(name, val.getClass().getName(), new CacheEntryValueFieldValue(val)));
        List<ICacheEntryValueField> resultFields = new ArrayList<>();
        fields.forEach(field ->
            resultFields.add(new CacheEntryValueField(field.getName(),
                    TypesResolver.toAvroType(field.getTypeClassName()), field.getFieldValue())));

        return new CacheDataTransformer(new CacheData(cacheData.getCacheName(), cacheData.getCacheEntryKey(),
                new CacheEntryValue(resultFields)));
    }

    public ICacheData build() {
        return cacheData;
    }
}
