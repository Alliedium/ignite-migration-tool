package org.alliedium.ignite.migration.patchtools;

public interface ITransformer<DATA> {

    ITransformer<DATA> addField(String name, Object val);

    ITransformer<DATA> removeField(String name);

    ITransformer<DATA> convertFieldsToAvro();

    DATA build();
}
