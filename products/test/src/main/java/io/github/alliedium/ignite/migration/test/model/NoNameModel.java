package io.github.alliedium.ignite.migration.test.model;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Objects;

public class NoNameModel {

    private final byte[] byteArray;
    private final Timestamp timestamp;

    public NoNameModel(byte[] byteArray, Timestamp timestamp) {
        this.byteArray = byteArray;
        this.timestamp = timestamp;
    }

    public byte[] getByteArray() {
        return byteArray;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NoNameModel that = (NoNameModel) o;

        if (!Arrays.equals(byteArray, that.byteArray)) return false;
        return Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(byteArray);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }
}
