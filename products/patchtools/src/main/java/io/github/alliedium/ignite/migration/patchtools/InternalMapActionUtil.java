package io.github.alliedium.ignite.migration.patchtools;

import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

class InternalMapActionUtil {

    static PCollection<Row> map(PCollectionProvider pCollectionProvider,
                             RowElementsProcessor rowElementsProcessor) {
        PCollection<Row> pCollection = pCollectionProvider.getPCollection();
        return pCollection.apply(ParDo.of(rowElementsProcessor))
                .setCoder(pCollection.getCoder());
    }
}
