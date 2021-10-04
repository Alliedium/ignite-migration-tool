package com.alliedium.ignite.migration.patchtools;

public interface TransformAction<OUT> {
    OUT execute();
}
