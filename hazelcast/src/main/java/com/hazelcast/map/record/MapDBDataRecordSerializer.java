package com.hazelcast.map.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.mapdb.Serializer;

import com.hazelcast.map.DefaultRecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.MapDBDataSerializer;
import com.hazelcast.nio.serialization.SerializationService;

public class MapDBDataRecordSerializer implements Serializer<DataRecord>, Serializable {

    private static final long serialVersionUID = 5270107668795251613L;

    private transient MapDBDataSerializer ds;
    private transient DefaultRecordStore defaultRecordStore;

    public MapDBDataRecordSerializer(SerializationService serializationService, DefaultRecordStore defaultRecordStore) {
        ds = new MapDBDataSerializer(serializationService);
        this.defaultRecordStore = defaultRecordStore;
    }
    
    @Override
    public void serialize(DataOutput dataOutput, DataRecord dataRecord) throws IOException {
        // TODO: it would be nice not to have to embed the key within the value also, but this requires a MapDB API change, and it should be small (you would hope)
        ds.serialize(dataOutput, dataRecord.getKey());
        ds.serialize(dataOutput, dataRecord.getValue());
        dataOutput.writeLong(dataRecord.getVersion());
        dataOutput.writeBoolean(dataRecord.getStatistics() != null);
    }

    @Override
    public DataRecord deserialize(DataInput dataInput, int i) throws IOException {
        Data key = ds.deserialize(dataInput, -1);
        Data value = ds.deserialize(dataInput, -1);
        long version = dataInput.readLong();
        boolean stats = dataInput.readBoolean();
        DataRecord dataRecord = new DataRecord(defaultRecordStore, key, value, stats);
        dataRecord.setVersion(version);
        return dataRecord;
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}