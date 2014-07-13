package com.hazelcast.nio.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class MapDBLongSerializer implements org.mapdb.Serializer<Long>, Serializable {

    private static final long serialVersionUID = -5987929153384667667L;

    @Override
    public void serialize(DataOutput dataOutput, Long data) throws IOException {
        dataOutput.writeLong(data);
    }

    @Override
    public Long deserialize(DataInput dataInput, int i) throws IOException {
        return dataInput.readLong();
    }

    @Override
    public int fixedSize() {
        return 8;
    }
}
