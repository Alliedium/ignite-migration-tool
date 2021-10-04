package com.alliedium.ignite.migration.patchtools;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public interface PCollectionProvider {
    PCollection<Row> getPCollection();
}
