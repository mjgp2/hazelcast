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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

/**
 * @author ali 3/1/13
 */
public class MapDbCollectionWrapper implements Collection<MultiMapDataRecord> {

    

    private final NodeEngine nodeEngine;
    private final Collection<MultiMapDataRecord> delegate;
    private MultiMapContainer container;

    public MapDbCollectionWrapper(Collection<MultiMapDataRecord> delegate, NodeEngine nodeEngine, MultiMapContainer container) {
        this.nodeEngine = nodeEngine;
        this.container = container;
        this.delegate = delegate;
    }
    
    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean contains(Object o) {

        if (!(o instanceof MultiMapDataRecord)) {
            return false;
        }

        // set the hash
        getData((MultiMapDataRecord) o);

        return delegate.contains(o);
    }

    @Override
    public Iterator<MultiMapDataRecord> iterator() {
        return new Iterator<MultiMapDataRecord>() {

            private Iterator<MultiMapDataRecord> iterator = delegate.iterator();
            private MultiMapDataRecord current;

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public MultiMapDataRecord next() {
                current = iterator.next();
                return current;
            }

            @Override
            public void remove() {
                // don't delegate to iterator.remove() because we want to update the count
                MapDbCollectionWrapper.this.remove(current);
            }
        };
    }

    @Override
    public Object[] toArray() {
        ArrayList<MultiMapDataRecord> temp = new ArrayList<MultiMapDataRecord>();
        for (MultiMapDataRecord m : this) {
            temp.add(m);
        }
        Object[] arr = new Object[temp.size()];
        temp.toArray(arr);
        return arr;
    }

    @Override
    public <T> T[] toArray(T[] a) {
        ArrayList<MultiMapDataRecord> temp = new ArrayList<MultiMapDataRecord>();
        for (MultiMapDataRecord m : this) {
            temp.add(m);
        }
        temp.toArray(a);
        return a;
    }

    @Override
    public boolean add(MultiMapDataRecord e) {

        if ( ! delegate.add(e) ) {
            return false;
        }
        
        if ( container.getConfig().isBinary() ) {
            // replace the data value with a pointer
            container.writeData(e);
        }
        
        return true;
    }

    @Override
    public boolean remove(Object o) {

        if (!(o instanceof MultiMapDataRecord)) {
            return false;
        }
        
        MultiMapDataRecord r = (MultiMapDataRecord) o;
        if ( r.getObject() != null && !(r.getObject() instanceof Data) ) {
            throw new IllegalArgumentException("Expected Data class");
        }
        if ( r.getHash() == -1 ) {
            r.setHash( ((Data)r.getObject()).hashCode());
        }
        
        Iterator<MultiMapDataRecord> i = delegate.iterator();
        while ( i.hasNext() ) {
            
            MultiMapDataRecord record = i.next();
            record.setDataMap(container.getDataMap());
            
            if ( record.equals(o) ) {
                i.remove();
                // TODO: would be better to have a method to remove and discard
                container.getDataMap().remove( record.getObjectId() );
                return true;
            }
        }
        
        return false;
    }


    private Data getData(MultiMapDataRecord e) {
        if ( e.getObject() == null ) {
            throw new IllegalArgumentException("Trying to get loaded object but not loaded");
        }
        Data d = (Data) (e.getObject() instanceof Data ? e.getObject() : nodeEngine.toData(e.getObject()));
        if ( e.getHash() == -1 ) {
            e.setHash(d.hashCode());     
        }
        e.setDataMap(container.getDataMap());
        return d;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends MultiMapDataRecord> c) {
        boolean ret = false;
        for (MultiMapDataRecord k : c) {
            ret = add(k) || ret;
        }
        return ret;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        
        Set<MultiMapDataRecord> copy = new HashSet<MultiMapDataRecord>(delegate);
        delegate.removeAll(copy);
        
        for ( MultiMapDataRecord r : copy ) {
            container.getDataMap().remove(r.getObjectId());
        }
    }

}