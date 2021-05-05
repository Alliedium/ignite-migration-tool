package org.alliedium.ignite.migration.patchtools;

public interface IMetaTransformer<META> {

    IMetaTransformer<META> addFieldType(String name, Class<?> clazz);

    META build();
}

