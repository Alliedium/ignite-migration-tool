package io.github.alliedium.ignite.migration.patchtools;

import org.apache.beam.sdk.values.Row;

import java.io.Serializable;
import java.util.function.Function;

public interface RowFunction extends Function<Row, Row>, Serializable {
}
