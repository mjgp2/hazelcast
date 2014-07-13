package com.hazelcast.multimap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import com.hazelcast.nio.serialization.MapDBDataSerializer;
import com.hazelcast.nio.serialization.SerializationService;

public class MapDBMultiMapRecordSerializer implements org.mapdb.Serializer<MultiMapRecord>, Serializable {

    private static final long serialVersionUID = -5987929153384667667L;

    public static final int NO_CLASS_ID = 0;
    
    private transient final SerializationService serializationService;
    private transient MapDBDataSerializer mapDBDataSerializer;

    public MapDBMultiMapRecordSerializer(SerializationService serializationService) {
        this.serializationService = serializationService;
        mapDBDataSerializer = new MapDBDataSerializer(serializationService);
    }
    
    @Override
    public void serialize(DataOutput out, MultiMapRecord data) throws IOException {
        out.writeLong(data.getRecordId());
        //mapDBDataSerializer.serialize(out, (Data) (data.getObject() instanceof Data ? data.getObject() : serializationService.toData(data.getObject())));
    }

    @Override
    public MultiMapRecord deserialize(DataInput in, int i) throws IOException {
        
        long id = in.readLong();
        //Data data = mapDBDataSerializer.deserialize(in, i);
        
        MultiMapRecord record = new MultiMapRecord(id, null);
        
        return record;
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
