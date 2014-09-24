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

package com.hazelcast.multimap.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

import com.hazelcast.config.MultiMapConfig;

public class MultiMapWrapper {

    private final Collection<MultiMapRecord> collection;

    private int hits;

    private long version = -1;

    private MultiMapContainer container;

    public MultiMapWrapper(Collection<? extends MultiMapRecord> collection, MultiMapContainer container) {
        this.collection = (Collection<MultiMapRecord>) collection;
        this.container = container;
    }

    public Collection<MultiMapRecord> getCollection(boolean copyOf) {
        if (copyOf) {
            return getCopyOfCollection();
        }
        return collection;
    }

    private Collection<MultiMapRecord> getCopyOfCollection() {
        if ( container.getConfig().isBinary() ) {
            
            final Collection<MultiMapRecord> collection;
            if ( isSet() ) {
                collection = new HashSet<MultiMapRecord>();
            } else if ( isList() ) {
                collection = new LinkedList<MultiMapRecord>();
            } else {
                throw new IllegalArgumentException("No Matching CollectionProxyType!");
            }
            
            for ( MultiMapRecord r : this.collection ) {
                Object loadedObject = ((MultiMapDataRecord)r).loadObject();
                collection.add(new MultiMapDataRecord(r.getRecordId(), loadedObject));
            }
            
            return collection;
        }
        
        final Collection<MultiMapRecord> collection;
        if ( isSet() ) {
            collection = new HashSet<MultiMapRecord>(this.collection);
        } else if ( isList() ) {
            collection = new LinkedList<MultiMapRecord>(this.collection);
        } else {
            throw new IllegalArgumentException("No Matching CollectionProxyType!");
        }
        
        return collection;
        
    }

    private boolean isList() {
        return container.getConfig().getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.LIST);
    }

    private boolean isSet() {
        return container.getConfig().getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.SET);
    }
    
    public void incrementHit() {
        hits++;
    }

    public int getHits() {
        return hits;
    }

    public boolean containsRecordId(long recordId) {
        for (MultiMapRecord record : collection) {
            if (record.getRecordId() == recordId) {
                return true;
            }
        }
        return false;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long incrementAndGetVersion() {
        return ++version;
    }
    
    public void destroy() {
        collection.clear();
    }

}
