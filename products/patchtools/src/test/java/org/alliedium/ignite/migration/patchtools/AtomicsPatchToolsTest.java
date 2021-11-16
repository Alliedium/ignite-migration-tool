package org.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.Controller;
import org.alliedium.ignite.migration.serializer.utils.FieldNames;
import org.apache.beam.sdk.values.Row;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AtomicsPatchToolsTest extends BaseTest {

    @Test
    public void testAtomicPatchTools(Method method) {
        String methodName = method.getName();
        String atomicName1 = methodName + 1;
        String atomicName2 = methodName + 2;
        String atomicName3 = methodName + 3;

        clientAPI.getIgnite().atomicLong(atomicName1, 1, true);
        clientAPI.getIgnite().atomicLong(atomicName2, 1, true);
        clientAPI.getIgnite().atomicLong(atomicName3, 1, true);

        controller = new Controller(clientAPI.getIgnite(),
                () -> Stream.of(atomicName1, atomicName2, atomicName3).collect(Collectors.toList()));

        controller.serializeDataToAvro(source.getPath());

        PatchContext context = new PatchContext(source, destination);
        context.prepare();

        Map<String, Long> map = new HashMap<>();
        map.put(atomicName1, (long) 1);
        map.put(atomicName2, (long) 1);
        map.put(atomicName3, (long) 1);
        clientAPI.clearIgniteAndCheckIgniteIsEmpty(map);

        TransformAction<TransformAtomicsOutput> action = new SelectAtomicsAction(context)
                .from(source.getPath().toString());
        TransformAction<TransformAtomicsOutput> duplicatedAtomicAction = new MapAtomicsAction(action).map(row -> {
            String atomicName = row.getValue(FieldNames.IGNITE_ATOMIC_LONG_NAME_FIELD_NAME);
            return Row.fromRow(row)
                    .withFieldValue(FieldNames.IGNITE_ATOMIC_LONG_NAME_FIELD_NAME, atomicName + 1)
                    .build();
        });

        action = new MapAtomicsAction(action).map(row -> {
            String atomicName = row.getValue(FieldNames.IGNITE_ATOMIC_LONG_NAME_FIELD_NAME);
            long val = row.getValue(FieldNames.IGNITE_ATOMIC_LONG_VALUE_FIELD_NAME);
            if (atomicName1.equals(atomicName)) {
                val = 2;
                map.put(atomicName, val);
            }
            return Row.fromRow(row)
                    .withFieldValue(FieldNames.IGNITE_ATOMIC_LONG_VALUE_FIELD_NAME, val)
                    .build();
        });

        action = new MergeRowsAtomicsAction(action, duplicatedAtomicAction);
        new AtomicsWriter(action).writeTo(destination.getPath().toString());

        context.getPipeline().run().waitUntilFinish();

        controller.deserializeDataFromAvro(destination.getPath());

        long atomicName1Val = clientAPI.getIgnite().atomicLong(atomicName1, 1, false).get();
        Assert.assertEquals(atomicName1Val, 2);

        map.put(atomicName1 + 1, (long) 1);
        map.put(atomicName2 + 1, (long) 1);
        map.put(atomicName3 + 1, (long) 1);

        clientAPI.clearIgniteAndCheckIgniteIsEmpty(map);
    }
}
