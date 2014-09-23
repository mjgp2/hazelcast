package com.hazelcast.storage;

import com.hazelcast.nio.serialization.Data;

public interface MapDbStorage {

    long put(Data data);
    void put(long id, Data data);
    Data get(long id);
    boolean contains(long id);
    void remove(long id);
    void destroy();
}
