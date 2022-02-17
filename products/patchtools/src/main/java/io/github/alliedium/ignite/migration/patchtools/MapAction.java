package io.github.alliedium.ignite.migration.patchtools;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.Objects;

public class MapAction implements TransformAction<TransformOutput> {
    private final TransformAction<TransformOutput> action;
    private final RowElementsProcessor rowElementsProcessor;

    private MapAction(Builder builder) {
        this.action = builder.action;
        this.rowElementsProcessor = builder.rowElementsProcessor;
    }

    @Override
    public TransformOutput execute() {
        TransformOutput output = action.execute();
        PCollection<Row> pCollection = InternalMapActionUtil.map(output, rowElementsProcessor);
        return new TransformOutput.Builder(output)
                .setPCollection(pCollection)
                .build();
    }

    public static class Builder {
        private TransformAction<TransformOutput> action;
        private RowElementsProcessor rowElementsProcessor;

        public Builder action(TransformAction<TransformOutput> action) {
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

        public MapAction build() {
            validate();
            return new MapAction(this);
        }
    }
}
