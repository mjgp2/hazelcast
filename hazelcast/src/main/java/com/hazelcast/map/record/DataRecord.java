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

package com.hazelcast.map.record;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.storage.MapDbStorage;

class DataRecord extends AbstractRecord<Data> implements MapDbRecord {

    // TODO: guess
    private static final long MAP_DB_ENTRY_HEAP_SIZE = 32;

    private MapDbStorage storage;
    private long dataId = -1;

    DataRecord(MapDbStorage storage, Data keyData, Data value) {
        super(keyData);
        this.storage = storage;
        setValue(value);
    }

    DataRecord() {
    }
    
    @Override
    public long getDataId(){
        return dataId;
    }

    /*
    * get record size in bytes.
    *
    * */
    @Override
    public long getCost() {
        long size = super.getCost();
        // add value size.
        size += MAP_DB_ENTRY_HEAP_SIZE;
        return size;
    }

    public Data getValue() {
        if ( dataId >= 0 ) {
            return storage.get(dataId);
        }
        return null;
    }

    public void setValue(Data o) {
        
        // delete the old value out of mapdb
        invalidate();
        
        if ( o == null ) {
            return;
        }
    
        // add in the new value
        dataId = storage.put(o);
    }

    public void invalidate() {
        if ( dataId >= 0 ) {
            storage.remove(dataId);
            dataId = -1;
        }
    }
    
    @Override
    public String toString() {
        return "DataRecord{" + "key=" + key + ",dataId="+dataId+'}';
    }

}
