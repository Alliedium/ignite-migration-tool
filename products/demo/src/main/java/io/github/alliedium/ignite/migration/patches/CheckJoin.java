package io.github.alliedium.ignite.migration.patches;

import io.github.alliedium.ignite.migration.demotools.CacheNames;
import io.github.alliedium.ignite.migration.demotools.LoadAndCheckDataCommon;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;

import java.util.Collection;

public class CheckJoin extends LoadAndCheckDataCommon {
    public CheckJoin(String[] args) {
        super(args);
    }

    @Override
    protected void checkData() {
        if (clientAPI.getIgnite().cacheNames().contains(CacheNames.FIRST)) {
            throw new AssertionError("first cache should not exist");
        }
        if (clientAPI.getIgnite().cacheNames().contains(CacheNames.SECOND)) {
            throw new AssertionError("second cache should not exist");
        }

        clientAPI.getIgnite().cache(CacheNames.THIRD).withKeepBinary()
                .query(new ScanQuery<Object, BinaryObject>())
                .forEach(entry -> {
                    assertFieldsExistAndNotEmpty(entry.getValue(), "id", "tickets", "expense", "income");
                });
    }

    private void assertFieldsExistAndNotEmpty(BinaryObject binaryObject, String... fields) {
        Collection<String> binaryObjectFieldNames = binaryObject.type().fieldNames();
        for (String field : fields) {
            if (!binaryObjectFieldNames.contains(field)) {
                throw new AssertionError(String.format("field [%s] should be present", field));
            }
            if (binaryObject.field(field) == null) {
                throw new AssertionError(String.format("field [%s] has value null or empty", field));
            }
        }
    }

    public static void main(String[] args) {
        new CheckJoin(args).execute();
    }
}
