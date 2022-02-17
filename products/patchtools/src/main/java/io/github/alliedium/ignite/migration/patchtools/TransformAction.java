package io.github.alliedium.ignite.migration.patchtools;

public interface TransformAction<OUT> {
    OUT execute();
}
