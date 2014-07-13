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

import com.hazelcast.map.DefaultRecordStore;
import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.serialization.Data;

public /*final*/ class DataRecord extends AbstractRecord<Data> implements Record<Data> {

    // TODO: this is a total guess
    private static final int MAP_DB_ENTRY_HEAP_SIZE = 16;
    
    protected Data value;
    private DefaultRecordStore recordStore;

    public DataRecord(RecordStore recordStore, Data keyData, Data value, boolean statisticsEnabled) {
        super(keyData, false);
        DefaultRecordStore defaultRecordStore = (DefaultRecordStore) recordStore;
        if ( statisticsEnabled ) {
            this.statistics = defaultRecordStore.getRecordStatistics(keyData);
        }
        this.recordStore = defaultRecordStore;
        this.value = value;
    }

    public DataRecord() {
    }

    /*
    * get record size in bytes.
    *
    * */
    @Override
    public long getCost() {
        long size = super.getCost();

        // add value size.
        size += 4 + MAP_DB_ENTRY_HEAP_SIZE;
        return size;
    }

    public Data getValue() {
        return value;
    }

    public void setValue(Data o) {
        value = o;
        
        recordStore.updateRecord(this);
    }

    public void invalidate() {
        value = null;
    }
}
