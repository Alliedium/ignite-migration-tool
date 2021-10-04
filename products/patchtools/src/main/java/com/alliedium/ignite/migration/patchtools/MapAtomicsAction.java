package com.alliedium.ignite.migration.patchtools;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class MapAtomicsAction implements TransformAction<TransformAtomicsOutput> {
    private final TransformAction<TransformAtomicsOutput> transformAction;
    private RowElementsProcessor rowElementsProcessor;

    public MapAtomicsAction(TransformAction<TransformAtomicsOutput> transformAction) {
        this.transformAction = transformAction;
    }

    public MapAtomicsAction map(RowFunction rowFunction) {
        this.rowElementsProcessor = new RowElementsProcessor(rowFunction);
        return this;
    }

    @Override
    public TransformAtomicsOutput execute() {
        TransformAtomicsOutput output = transformAction.execute();
        PCollection<Row> pCollection = InternalMapActionUtil
                .map(output::getPCollection, rowElementsProcessor);
        return new TransformAtomicsOutput(pCollection, output.getSchema());
    }
}
