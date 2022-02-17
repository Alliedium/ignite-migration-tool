package io.github.alliedium.ignite.migration.test.model;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;

import java.util.Objects;

public class PartialFieldBinarylizable implements Binarylizable {
    private String name;
    private String type;
    private String value;
    private String consumer;

    public PartialFieldBinarylizable(String name, String type, String value, String consumer) {
        this.name = name;
        this.type = type;
        this.value = value;
        this.consumer = consumer;
    }

    @Override
    public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        writer.writeString("specialName", name);
        writer.writeString("specialType", type);
        writer.writeString("specialValue", value);
    }

    @Override
    public void readBinary(BinaryReader reader) throws BinaryObjectException {
        this.name = reader.readString("specialName");
        this.type = reader.readString("specialType");
        this.value = reader.readString("specialValue");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartialFieldBinarylizable that = (PartialFieldBinarylizable) o;

        if (!Objects.equals(name, that.name)) return false;
        if (!Objects.equals(type, that.type)) return false;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}
