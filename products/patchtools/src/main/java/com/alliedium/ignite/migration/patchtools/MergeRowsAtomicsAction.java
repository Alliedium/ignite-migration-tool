package com.alliedium.ignite.migration.patchtools;

import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;

public class MergeRowsAtomicsAction implements TransformAction<TransformAtomicsOutput> {
    private final TransformAction<TransformAtomicsOutput> firstAction;
    private final TransformAction<TransformAtomicsOutput> secondAction;

    public MergeRowsAtomicsAction(TransformAction<TransformAtomicsOutput> firstAction,
                                  TransformAction<TransformAtomicsOutput> secondAction) {
        this.firstAction = firstAction;
        this.secondAction = secondAction;
    }

    @Override
    public TransformAtomicsOutput execute() {
        TransformAtomicsOutput out1 = firstAction.execute();
        TransformAtomicsOutput out2 = secondAction.execute();

        if (!out1.getSchema().equals(out2.getSchema())) {
            throw new IllegalArgumentException(
                    String.format("Cannot merge beam rows with different schema, first schema: %s\nsecond schema: %s",
                            out1.getSchema(), out2.getSchema()));
        }

        PCollectionList<Row> pCollectionList = PCollectionList.of(out1.getPCollection())
                .and(out2.getPCollection());
        PCollection<Row> merged = pCollectionList.apply(Flatten.pCollections());

        return new TransformAtomicsOutput(merged, out1.getSchema());
    }
}
