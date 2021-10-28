package org.alliedium.ignite.migration.dao.converters;

import org.alliedium.ignite.migration.dto.CacheEntryValue;
import org.alliedium.ignite.migration.dto.CacheEntryValueField;
import org.alliedium.ignite.migration.dto.ICacheEntryValue;

import java.util.Collections;

public class PlainEntryValueWrapper {

    public static ICacheEntryValue wrap(Object obj, String fieldName) {
        return new CacheEntryValue(Collections.singletonList(
                new CacheEntryValueField.Builder()
                        .setName(fieldName)
                        .setTypeClassName(TypesResolver.toAvroType(obj.getClass().getName()))
                        .setValue(obj)
                        .build()));
    }
}
