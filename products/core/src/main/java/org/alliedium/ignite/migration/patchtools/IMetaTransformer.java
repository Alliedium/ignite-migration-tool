package org.alliedium.ignite.migration.patchtools;

public interface IMetaTransformer<META> {

    IMetaTransformer<META> addFieldType(String name, Class<?> clazz);

    IMetaTransformer<META> removeFieldType(String name);

    META build();
}

