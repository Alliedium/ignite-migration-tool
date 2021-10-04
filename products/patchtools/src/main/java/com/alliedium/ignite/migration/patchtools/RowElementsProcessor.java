package com.alliedium.ignite.migration.patchtools;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

import java.io.Serializable;

public class RowElementsProcessor extends DoFn<Row, Row> implements Serializable {
    private final RowFunction rowConsumer;

    public RowElementsProcessor(RowFunction rowConsumer) {
        this.rowConsumer = rowConsumer;
    }

    @ProcessElement
    public void process(ProcessContext c) {
        c.output(rowConsumer.apply(c.element()));
        //c.output(c.element());
    }
}
