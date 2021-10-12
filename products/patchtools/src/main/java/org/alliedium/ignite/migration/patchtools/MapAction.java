package org.alliedium.ignite.migration.patchtools;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class MapAction implements TransformAction<TransformOutput> {
    private final TransformAction<TransformOutput> transformAction;
    private RowElementsProcessor rowElementsProcessor;

    public MapAction(TransformAction<TransformOutput> transformAction) {
        this.transformAction = transformAction;
    }

    public MapAction map(RowFunction rowFunction) {
        this.rowElementsProcessor = new RowElementsProcessor(rowFunction);
        return this;
    }

    @Override
    public TransformOutput execute() {
        TransformOutput output = transformAction.execute();
        PCollection<Row> pCollection = InternalMapActionUtil.map(output, rowElementsProcessor);
        return new TransformOutput.Builder(output)
                .setPCollection(pCollection)
                .build();
    }
}
