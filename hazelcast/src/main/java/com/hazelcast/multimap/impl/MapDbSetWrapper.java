package com.hazelcast.multimap.impl;

import java.util.Collection;
import java.util.Set;

import com.hazelcast.spi.NodeEngine;

public class MapDbSetWrapper extends MapDbCollectionWrapper implements Set<MultiMapDataRecord> {

	public MapDbSetWrapper(Collection<MultiMapDataRecord> delegate,
			NodeEngine nodeEngine, MultiMapContainer container) {
		super(delegate, nodeEngine, container);
	}

}
