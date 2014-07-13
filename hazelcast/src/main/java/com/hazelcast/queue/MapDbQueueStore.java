package com.hazelcast.queue;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.hazelcast.core.QueueStore;

public class MapDbQueueStore implements QueueStore<byte[]>, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 4319059189762343369L;

    final Map<Long, byte[]> map;

    MapDbQueueStore(Map<Long, byte[]> map) {
        this.map = map;
    }

    public void store(Long key, byte[] value) {
        map.put(key, value);
    }

    public void storeAll(Map<Long, byte[]> map) {
        map.putAll(map);
    }

    public void delete(Long key) {
        map.remove(key);
    }

    public void deleteAll(Collection<Long> keys) {
        for (Object key : keys) {
            map.remove(key);
        }
    }

    public byte[] load(Long key) {
        return map.get(key);
    }

    public Map<Long, byte[]> loadAll(Collection<Long> keys) {
        // TODO: THIS COULD USE AN IMMUTABLE MAP
        Map<Long, byte[]> m = new HashMap<Long, byte[]>();
        for (Long key : keys) {
            m.put(key, map.get(key));
        }
        return m;
    }

    public Set<Long> loadAllKeys() {
        return map.keySet();
    }

}
