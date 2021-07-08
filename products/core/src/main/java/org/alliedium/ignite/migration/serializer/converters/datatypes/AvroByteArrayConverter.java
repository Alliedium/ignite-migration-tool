package org.alliedium.ignite.migration.serializer.converters.datatypes;

import java.nio.ByteBuffer;
import org.apache.avro.util.Utf8;

/**
 * Converter required for avro-serialization of the byte-array values.
 * Values are being converted to String. Reverse conversion is also available.
 * Fields with converted byte-array value are also being correspondingly marked in avro schema.
 *
 */
public class AvroByteArrayConverter implements IAvroDerivedTypeConverter {

    public ByteBuffer convertForAvro(Object fieldData) {
        assert fieldData instanceof byte[];
        return ByteBuffer.wrap((byte[]) fieldData);
    }

    public byte[] convertFromAvro(Object fieldData) {
        assert (fieldData instanceof ByteBuffer) || (fieldData instanceof Utf8);
        // todo: fix this, the problem is that the Avro Utf8 object has doc: byte[]
        if (fieldData instanceof Utf8) {
            return ((Utf8) fieldData).getBytes();
        }

        return ((ByteBuffer) fieldData).array();
    }

}
