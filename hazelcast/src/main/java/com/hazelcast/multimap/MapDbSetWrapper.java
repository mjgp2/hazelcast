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
import java.util.concurrent.atomic.AtomicInteger;

import org.mapdb.Fun;
import org.mapdb.Fun.Tuple3;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

/**
 * @author ali 3/1/13
 */
public class MapDbSetWrapper implements Set<MultiMapRecord> {

    private AtomicInteger counter = new AtomicInteger();
    private final NavigableSet<Tuple3<Data, Data, MultiMapRecord>> navigableSet;
    private final Data key;
    private NodeEngine nodeEngine;

    public MapDbSetWrapper(NodeEngine nodeEngine,
            NavigableSet<Tuple3<Data, Data, MultiMapRecord>> navigableSet2,
            Data key) {
        this.navigableSet = navigableSet2;
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

        MultiMapRecord e = (MultiMapRecord) o;

        Data d = (Data) (e.getObject() instanceof Data ? e.getObject()
                : nodeEngine.toData(e.getObject()));

        return Fun.filter(navigableSet, key, d).iterator().hasNext();
    }

    public static <A, B, C> Iterable<Fun.Tuple3<A, B, C>> filter(
            final NavigableSet<Fun.Tuple3<A, B, C>> secondaryKeys, final A a,
            final B b) {
        return new Iterable<Fun.Tuple3<A, B, C>>() {
            @Override
            public Iterator<Fun.Tuple3<A, B, C>> iterator() {
                // use range query to get all values
                @SuppressWarnings({ "unchecked", "rawtypes" })
                final Iterator<Fun.Tuple3> iter = ((NavigableSet) secondaryKeys)
                        .subSet(Fun.t3(a, b, null),
                                Fun.t3(a, b == null ? Fun.HI() : b, Fun.HI()))
                        .iterator();

                return new Iterator<Fun.Tuple3<A, B, C>>() {
                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @SuppressWarnings("unchecked")
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

            private Iterator<Tuple3<Data, Data, MultiMapRecord>> iterator = filter(navigableSet, key, null).iterator();
            private Tuple3<Data, Data, MultiMapRecord> next;

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public MultiMapRecord next() {
                next = iterator.next();
                if ( next == null ) {
                    return null;
                }
                next.c.setObject(next.b);
                return next.c;
            }

            @Override
            public void remove() {
                // don't delegate to iterator.next() because we want to update the count
                MapDbSetWrapper.this.remove(next.c);
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
        ArrayList<MultiMapRecord> temp = new ArrayList<MultiMapRecord>(this);
        temp.toArray(a);
        return a;
    }

    @Override
    public boolean add(MultiMapRecord e) {

        if (!(e.getObject() instanceof Data)) {
            e.setObject(nodeEngine.toData(e.getObject()));
        }

        boolean added = navigableSet.add(Fun.t3(key, (Data) e.getObject(), e));
        if (added) {
            counter.incrementAndGet();
        }
        return added;
    }

    @Override
    public boolean remove(Object o) {

        if (!(o instanceof MultiMapRecord)) {
            return false;
        }

        MultiMapRecord e = (MultiMapRecord) o;

        Data d = (Data) (e.getObject() instanceof Data ? e.getObject()
                : nodeEngine.toData(e.getObject()));

        Iterator<MultiMapRecord> iterator = Fun.filter(navigableSet, key, d)
                .iterator();
        if (!iterator.hasNext()) {
            return false;
        }

        MultiMapRecord removed = iterator.next();
        iterator.remove();
        counter.decrementAndGet();

        return true;
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
        boolean ret = false;
        for (Object o : c) {
            ret = remove(o) || ret;
        }
        return ret;
    }

    @Override
    public void clear() {
        for (Object o : Fun.filter(navigableSet, key, null)) {
            remove(o);
        }
    }

}
