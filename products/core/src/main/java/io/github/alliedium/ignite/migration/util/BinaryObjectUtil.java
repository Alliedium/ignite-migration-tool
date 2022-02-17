package io.github.alliedium.ignite.migration.util;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cache.affinity.AffinityKey;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BinaryObjectUtil {

    private static final List<String> affinityKeyFields = Collections.unmodifiableList(Arrays.asList("key", "affKey"));

    public static boolean isAffinityKey(BinaryObject binaryObject) {
        try {
            binaryObject.type().fieldNames();
            return binaryObject.type().typeName().equals(AffinityKey.class.getName());
        } catch (BinaryObjectException e) {
            return affinityKeyFields.stream().allMatch(binaryObject::hasField);
        }
    }

    public static String getBinaryTypeName(BinaryObject binaryObject) {
        return isAffinityKey(binaryObject) ? AffinityKey.class.getName() : binaryObject.type().typeName();
    }
}
