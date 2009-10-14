/**
 * 
 */
package com.plexobject.docusearch.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * @author bhatti@plexobject.com
 * 
 */
public class SearchDocList implements List<SearchDoc> {
	private final int totalHits;
	private final List<SearchDoc> docs = new ArrayList<SearchDoc>();

	public SearchDocList(final int totalHits, final Collection<SearchDoc> c) {
		this.totalHits = totalHits;
		this.docs.addAll(c);
	}

	@Override
	public boolean add(final SearchDoc d) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void add(final int index, final SearchDoc d) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(final Collection<? extends SearchDoc> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(int index, Collection<? extends SearchDoc> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean contains(final Object o) {
		return docs.contains(o);
	}

	@Override
	public boolean containsAll(final Collection<?> c) {
		return docs.containsAll(c);
	}

	@Override
	public SearchDoc get(final int index) {
		return docs.get(index);
	}

	@Override
	public int indexOf(final Object o) {
		return docs.indexOf(o);
	}

	@Override
	public boolean isEmpty() {
		return docs.isEmpty();
	}

	@Override
	public Iterator<SearchDoc> iterator() {
		return docs.iterator();
	}

	@Override
	public int lastIndexOf(final Object o) {
		return docs.lastIndexOf(o);

	}

	@Override
	public ListIterator<SearchDoc> listIterator() {
		return docs.listIterator();
	}

	@Override
	public ListIterator<SearchDoc> listIterator(final int index) {
		return docs.listIterator(index);
	}

	@Override
	public boolean remove(final Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SearchDoc remove(final int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(final Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(final Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SearchDoc set(final int index, final SearchDoc element) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int size() {
		return docs.size();
	}

	@Override
	public List<SearchDoc> subList(final int fromIndex, final int toIndex) {
		return docs.subList(fromIndex, toIndex);
	}

	@Override
	public Object[] toArray() {
		return docs.toArray();
	}

	public int getTotalHits() {
		return totalHits;
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return docs.toArray(a);
	}

}
