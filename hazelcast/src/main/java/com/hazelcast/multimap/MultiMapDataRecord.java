/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.multimap;

import java.io.IOException;
import java.util.Map;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

/**
 * @author ali 1/23/13
 */
public class MultiMapDataRecord extends MultiMapRecord {

    private long objectId = -1;
    private long hash = -1;
    
    private Map<Long, Data> dataMap;

    public MultiMapDataRecord(long recordId, long objectId, long hash, Map<Long,Data> dataMap) {
        super(recordId, null);
        this.objectId = objectId;
        this.dataMap = dataMap;
        setHash(hash);
    }
    
    public MultiMapDataRecord() {
    }

    public MultiMapDataRecord(Object object) {
        super(object);
        setHash(object.hashCode());
        checkData(object);
    }

    public MultiMapDataRecord(long recordId, Object object) {
        super(recordId, object);
        setHash(object.hashCode());
        checkData(object);
    }
    
    
    private void checkData(Object object) {
        if ( ! ( object instanceof Data ) ) {
            throw new IllegalArgumentException("Expected a Data object");
        }
    }

    public Map<Long, Data> getDataMap() {
        return dataMap;
    }
    
    public void setDataMap(Map<Long, Data> dataMap) {
        this.dataMap = dataMap;
    }

    public Object getObject() {
        if ( object != null ) {
            return object;
        }
        if ( objectId >=0 ) {
            return loadObject();
        }
        return null;
    }
    
    public Object loadObject() {
        if ( object != null ) {
            return object;
        }
        if ( objectId < 0 ) {
            return null;
        }
        if ( dataMap == null ) {
            throw new RuntimeException("Map not linked");
        }
        return dataMap.get(objectId);
    }

    public long getObjectId() {
        return objectId;
    }

    public void setObjectId(Map<Long, Data> map, long objectId, long hash) {
        this.objectId = objectId;
        this.object = null;
        setDataMap(map);
        setHash(hash);
    }
    
    public long getHash() {
        return hash;
    }

    public void setHash(long hash) {
        // -1 is reserved
        this.hash = hash == -1 ? 0 : hash;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MultiMapDataRecord)) return false;

        MultiMapDataRecord record = (MultiMapDataRecord) o;

        if ( objectId != -1 && record.objectId != -1 && objectId == record.objectId ) {
            return true;
        }
        
        if ( hash == -1 || record.hash == -1 ) {
            throw new RuntimeException("Hash not set");
        }
        
        if ( hash != record.hash ) {
            return false;
        }
        
        return equals(loadObject(), record.loadObject());
    }
    
    private boolean equals(Object a, Object b) {
        return (a == b) || (a != null && a.equals(b));
    }

    public int hashCode() {
        return (int) hash;
    }

    /**
     * Loads the object into the heap
     */
    public void loadAndSetObject() {
        object = loadObject();
    }
    
    /**
     * Maintain wire compatability
     */
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(recordId);
        out.writeObject(loadObject());
    }
    
    @Override
    public void readData(ObjectDataInput in) throws IOException {
        recordId = in.readLong();
        object = in.readObject();
        setHash( object.hashCode() );
    }

}
