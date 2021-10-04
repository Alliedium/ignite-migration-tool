package com.alliedium.ignite.migration.patchtools;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.Collection;

public class ChangeClassNameAction implements TransformAction<TransformOutput> {
    private final TransformAction<TransformOutput> transformAction;
    private String fromClassName;
    private String toClassName;

    public ChangeClassNameAction(TransformAction<TransformOutput> transformAction) {
        this.transformAction = transformAction;
    }

    public ChangeClassNameAction changeClassName(String fromClassName, String toClassName) {
        this.fromClassName = fromClassName;
        this.toClassName = toClassName;
        return this;
    }

    @Override
    public TransformOutput execute() {
        TransformOutput output = transformAction.execute();

        Collection<QueryEntity> queryEntities = output.getQueryEntities();
        queryEntities.forEach(queryEntity -> {
            if (queryEntity.getValueType().equals(fromClassName)) {
                queryEntity.setValueType(toClassName);
            }
        });
        CacheConfiguration<Object, BinaryObject> cacheConfiguration = output.getCacheConfiguration();
        cacheConfiguration.setQueryEntities(queryEntities);

        return new TransformOutput.Builder(output)
                .setCacheConfiguration(cacheConfiguration)
                .setQueryEntities(queryEntities)
                .build();
    }
}
