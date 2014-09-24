package com.hazelcast.multimap.impl;

import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

import com.hazelcast.spi.NodeEngine;

public class MapDbListWrapper extends MapDbCollectionWrapper implements List<MultiMapDataRecord> {

	public MapDbListWrapper(Collection<MultiMapDataRecord> delegate,
			NodeEngine nodeEngine, MultiMapContainer container) {
		super(delegate, nodeEngine, container);
	}

	@Override
	public boolean addAll(int index, Collection<? extends MultiMapDataRecord> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MultiMapDataRecord get(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MultiMapDataRecord set(int index, MultiMapDataRecord element) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void add(int index, MultiMapDataRecord element) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MultiMapDataRecord remove(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int indexOf(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int lastIndexOf(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ListIterator<MultiMapDataRecord> listIterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ListIterator<MultiMapDataRecord> listIterator(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<MultiMapDataRecord> subList(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException();
	}

}
