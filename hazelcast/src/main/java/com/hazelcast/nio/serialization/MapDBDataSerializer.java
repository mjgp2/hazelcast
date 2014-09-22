package com.hazelcast.nio.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class MapDBDataSerializer implements org.mapdb.Serializer<Data>, Serializable {

    private static final long serialVersionUID = -5987929153384667667L;

    public static final int NO_CLASS_ID = 0;
    
    private transient final SerializationService serializationService;

    public MapDBDataSerializer(SerializationService serializationService) {
        this.serializationService = serializationService;
    }
    
    @Override
    public void serialize(DataOutput out, Data data) throws IOException {
        
        out.writeInt(data.getType());
        int size = data.getBuffer() == null ? 0 : data.getBuffer().length;
        out.writeInt(size);
        if ( size > 0 ) {
            out.write(data.getBuffer());
        }
        out.writeInt(data.getPartitionHash());
        
        ClassDefinition classDefinition = data.getClassDefinition();
        if (classDefinition != null) {
            out.writeInt(classDefinition.getClassId());
            out.writeInt(classDefinition.getFactoryId());
            out.writeInt(classDefinition.getVersion());
            byte[] classDefBytes = ((BinaryClassDefinition) classDefinition).getBinary();
            out.writeInt(classDefBytes.length);
            out.write(classDefBytes);
        } else {
            out.writeInt(NO_CLASS_ID);
        }
    }

    @Override
    public Data deserialize(DataInput in, int i) throws IOException {
        int type = in.readInt();
        int size = in.readInt();
        byte[] buf;
        if ( size == 0 ) {
            buf = null;
        } else {
            buf = new byte[size];
            in.readFully(buf);
        }
        Data d = new Data(type, buf);
        d.partitionHash = in.readInt();
        
        final int classId = in.readInt();
        if (classId != NO_CLASS_ID) {
            final int factoryId = in.readInt();
            final int version = in.readInt();
            SerializationContext context = serializationService.getSerializationContext();
            d.classDefinition = context.lookup(factoryId, classId, version);
            int classDefSize = in.readInt();
            if (d.classDefinition != null) {
                final int skipped = in.skipBytes(classDefSize);
                if (skipped != classDefSize) {
                    throw new HazelcastSerializationException("Not able to skip " + classDefSize + " bytes");
                }
            } else {
                byte[] classDefBytes = new byte[classDefSize];
                in.readFully(classDefBytes);
                d.classDefinition = context.createClassDefinition(factoryId, classDefBytes);
            }
        }
        
        return d;
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
