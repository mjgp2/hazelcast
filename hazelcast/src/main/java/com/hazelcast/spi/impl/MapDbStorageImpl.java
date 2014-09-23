package com.hazelcast.spi.impl;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.mapdb.DB;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.MapDBDataSerializer;
import com.hazelcast.storage.MapDbStorage;

final class MapDbStorageImpl implements MapDbStorage {

    private final NodeEngineImpl nodeEngineImpl;
    private final String mapDbName = UUID.randomUUID().toString();
    private final AtomicLong recordDataIdGenerator = new AtomicLong();
    private final DB mapDb;
    private final HTreeMap<Long, Data> map;

    MapDbStorageImpl(NodeEngineImpl nodeEngineImpl) {
        this.nodeEngineImpl = nodeEngineImpl;
        mapDb = this.nodeEngineImpl.node.hazelcastInstance.getMapDb();
        map = mapDb 
                .createHashMap(mapDbName)
                .keySerializer(Serializer.LONG)
                .valueSerializer(new MapDBDataSerializer(this.nodeEngineImpl.getSerializationService()))
                .counterEnable()
                .make();
    }
    
    @Override
    public void remove(long id) {
        map.remove(id);
    }

    @Override
    public void put(long id, Data data) {
        map.put(id, data);
    }

    @Override
    public long put(Data data) {
        long id = recordDataIdGenerator.incrementAndGet();
        put(id, data);
        return id;
    }

    @Override
    public Data get(long id) {
        return map.get(id);
    }

    @Override
    public void destroy() {
        mapDb.delete(mapDbName);
    }

    @Override
    public boolean contains(long id) {
        return map.containsKey(id);
    }
}