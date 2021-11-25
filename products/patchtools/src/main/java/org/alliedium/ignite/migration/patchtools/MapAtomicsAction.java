package org.alliedium.ignite.migration.patchtools;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.Objects;

public class MapAtomicsAction implements TransformAction<TransformAtomicsOutput> {
    private final TransformAction<TransformAtomicsOutput> action;
    private final RowElementsProcessor rowElementsProcessor;

    private MapAtomicsAction(Builder builder) {
        this.action = builder.action;
        this.rowElementsProcessor = builder.rowElementsProcessor;
    }

    @Override
    public TransformAtomicsOutput execute() {
        TransformAtomicsOutput output = action.execute();
        PCollection<Row> pCollection = InternalMapActionUtil
                .map(output::getPCollection, rowElementsProcessor);
        return new TransformAtomicsOutput(pCollection, output.getSchema());
    }

    public static class Builder {
        private TransformAction<TransformAtomicsOutput> action;
        private RowElementsProcessor rowElementsProcessor;

        public Builder action(TransformAction<TransformAtomicsOutput> action) {
            this.action = action;
            return this;
        }

        public Builder map(RowFunction rowFunction) {
            this.rowElementsProcessor = new RowElementsProcessor(rowFunction);
            return this;
        }

        private void validate() {
            Objects.requireNonNull(action, "parent action is null");
            Objects.requireNonNull(rowElementsProcessor, "map function not set");
        }

        public MapAtomicsAction build() {
            validate();
            return new MapAtomicsAction(this);
        }
    }
}
