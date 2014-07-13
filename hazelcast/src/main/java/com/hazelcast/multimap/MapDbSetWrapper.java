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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.mapdb.Fun;
import org.mapdb.Fun.Tuple3;

import com.carrotsearch.hppc.LongArrayList;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

/**
 * @author ali 3/1/13
 */
public class MapDbSetWrapper implements Set<MultiMapRecord> {

    private AtomicInteger counter = new AtomicInteger();
    private final NavigableSet<Tuple3<Data, Data, Long>> navigableSet;
    private final LongArrayList recordIdSet = new LongArrayList(1);
    private final Data key;
    private NodeEngine nodeEngine;

    public MapDbSetWrapper(NodeEngine nodeEngine,
            NavigableSet<Tuple3<Data, Data, Long>> navigableSet,
            Data key) {
        this.navigableSet = navigableSet;
        this.key = key;
        this.nodeEngine = nodeEngine;
    }

    @Override
    public int size() {
        return counter.get();
    }

    @Override
    public boolean isEmpty() {
        return counter.get() == 0;
    }

    @Override
    public boolean contains(Object o) {

        if (!(o instanceof MultiMapRecord)) {
            return false;
        }

        Data d = getData((MultiMapRecord) o);

        return contains(d);
    }

    private boolean contains(Data d) {
        return !subSet(navigableSet, key, d).isEmpty();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <A, B, C> SortedSet<Fun.Tuple3<A, B, C>> subSet( NavigableSet<Fun.Tuple3<A, B, C>> secondaryKeys, final A a, final B b) {
        return ((NavigableSet) secondaryKeys)
                .subSet(Fun.t3(a, b, null),
                        Fun.t3(a, b == null ? Fun.HI() : b, Fun.HI()));
    }
    
    public static <A, B, C> Iterable<Fun.Tuple3<A, B, C>> filter(
            final NavigableSet<Fun.Tuple3<A, B, C>> secondaryKeys, final A a,
            final B b) {
        return new Iterable<Fun.Tuple3<A, B, C>>() {
            @Override
            public Iterator<Fun.Tuple3<A, B, C>> iterator() {
                
                // use range query to get all values
                final Iterator<Fun.Tuple3<A, B, C>> iter = subSet(secondaryKeys, a, b).iterator();

                return new Iterator<Fun.Tuple3<A, B, C>>() {
                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public Fun.Tuple3<A, B, C> next() {
                        return iter.next();
                    }

                    @Override
                    public void remove() {
                        iter.remove();
                    }
                };
            }
        };

    }

    @Override
    public Iterator<MultiMapRecord> iterator() {
        return new Iterator<MultiMapRecord>() {

            private Iterator<Tuple3<Data, Data, Long>> iterator = filter(navigableSet, key, null).iterator();
            private Tuple3<Data, Data, Long> current;

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public MultiMapRecord next() {
                current = iterator.next();
                if ( current == null ) {
                    return null;
                }
                MultiMapRecord record = new MultiMapRecord(current.c, current.b);
                return record;
            }

            @Override
            public void remove() {
                // don't delegate to iterator.remove() because we want to update the count
                MapDbSetWrapper.this.remove(new MultiMapRecord(current.c, current.b));
            }
        };
    }

    @Override
    public Object[] toArray() {
        ArrayList<MultiMapRecord> temp = new ArrayList<MultiMapRecord>();
        for (MultiMapRecord m : this) {
            temp.add(m);
        }
        Object[] arr = new Object[temp.size()];
        temp.toArray(arr);
        return arr;
    }

    @Override
    public <T> T[] toArray(T[] a) {
        ArrayList<MultiMapRecord> temp = new ArrayList<MultiMapRecord>();
        for (MultiMapRecord m : this) {
            temp.add(m);
        }
        temp.toArray(a);
        return a;
    }

    @Override
    public boolean add(MultiMapRecord e) {

        // ensure the object is binary encoded
        Data data = getData(e);
        
        if ( contains(data) ) {
            return false;
        }

        navigableSet.add(Fun.t3(key, data, e.getRecordId()));
        recordIdSet.add(e.getRecordId());
        counter.incrementAndGet();
        return true;
    }

    @Override
    public boolean remove(Object o) {

        if (!(o instanceof MultiMapRecord)) {
            return false;
        }

        Data d = getData((MultiMapRecord) o);

        return remove(d);
    }

    private boolean remove(Data d) {
        Iterator<Tuple3<Data, Data, Long>> iterator = filter(navigableSet, key, d).iterator();
        if ( ! iterator.hasNext() ) {
            return false;
        }

        recordIdSet.removeFirstOccurrence(iterator.next().c);
        iterator.remove();
        counter.decrementAndGet();

        return true;
    }

    private Data getData(MultiMapRecord e) {
        Data d = (Data) (e.getObject() instanceof Data ? e.getObject() : nodeEngine.toData(e.getObject()));
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
    public boolean addAll(Collection<? extends MultiMapRecord> c) {
        boolean ret = false;
        for (MultiMapRecord k : c) {
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
        subSet(navigableSet, key, null).clear();
        counter.set(0);
    }

    public boolean containsRecordId(long recordId) {
        return recordIdSet.contains(recordId);
    }

}
