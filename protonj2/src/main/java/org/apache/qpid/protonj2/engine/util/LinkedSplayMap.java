/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.protonj2.engine.util;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.apache.qpid.protonj2.types.UnsignedInteger;

/**
 * @param <E> The type stored in the map entries
 */
public class LinkedSplayMap<E> extends SplayMap<E> {

    /**
     * A dummy entry in the circular linked list of entries in the map.
     * The first real entry is root.next, and the last is header.pervious.
     * If the map is empty, root.next == root && root.previous == root.
     */
    private final transient SplayedEntry<E> entries = new SplayedEntry<>();

    @Override
    public void clear() {
        super.clear();

        // Unlink all the entries and reset to no insertions state
        entries.linkNext = entries.linkPrev = entries;
    }

    @Override
    public Set<UnsignedInteger> keySet() {
        if (keySet == null) {
            keySet = new LinkedSplayMapKeySet();
        }
        return keySet;
    }

    @Override
    public Collection<E> values() {
        if (values == null) {
            values = new LinkedSplayMapValues();
        }
        return values;
    }

    @Override
    public Set<Entry<UnsignedInteger, E>> entrySet() {
        if (entrySet == null) {
            entrySet = new LinkedSplayMapEntrySet();
        }
        return entrySet;
    }

    @Override
    public void forEach(BiConsumer<? super UnsignedInteger, ? super E> action) {
        Objects.requireNonNull(action);

        SplayedEntry<E> root = this.entries;
        for (SplayedEntry<E> entry = root.linkNext; entry != root; entry = entry.linkNext) {
            action.accept(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        Objects.requireNonNull(action);

        SplayedEntry<E> root = this.entries;
        for (SplayedEntry<E> entry = root.linkNext; entry != root; entry = entry.linkNext) {
            action.accept(entry.getValue());
        }
    }

    @Override
    public void replaceAll(BiFunction<? super UnsignedInteger, ? super E, ? extends E> function) {
        Objects.requireNonNull(function, "The replacement function parameter cannot be null");

        final int initialModCount = modCount;
        for (SplayedEntry<E> e = entries.linkNext; e != entries; e = e.linkNext) {
            e.value = function.apply(e.getKey(), e.value);
        }

        if (modCount != initialModCount) {
            throw new ConcurrentModificationException();
        }
    }

    @Override
    protected void entryAdded(SplayedEntry<E> newEntry) {
        // Insertion ordering of the splayed map entries recorded here
        // and the list of entries doesn't change until an entry is removed.
        newEntry.linkNext = entries;
        newEntry.linkPrev = entries.linkPrev;
        entries.linkPrev.linkNext = newEntry;
        entries.linkPrev = newEntry;
    }

    @Override
    protected void entryDeleted(SplayedEntry<E> deletedEntry) {
        // Remove the entry from the insertion ordered entry list.
        deletedEntry.linkNext.linkPrev = deletedEntry.linkPrev;
        deletedEntry.linkPrev.linkNext = deletedEntry.linkNext;
        deletedEntry.linkNext = deletedEntry.linkPrev = null;
    }

    //----- Map Iterator implementation for EntrySet, KeySet and Values collections

    // Base class iterator that can be used for the collections returned from the Map
    private abstract class LinkedSplayMapIterator<T> implements Iterator<T> {

        private SplayedEntry<E> nextNode;
        private SplayedEntry<E> lastReturned;

        private int expectedModCount;

        public LinkedSplayMapIterator(SplayedEntry<E> startAt) {
            this.nextNode = startAt;
            this.expectedModCount = LinkedSplayMap.this.modCount;
        }

        @Override
        public boolean hasNext() {
            return nextNode != entries;
        }

        protected SplayedEntry<E> nextNode() {
            final SplayedEntry<E> entry = nextNode;

            if (nextNode == entries) {
                throw new NoSuchElementException();
            }
            if (expectedModCount != LinkedSplayMap.this.modCount) {
                throw new ConcurrentModificationException();
            }

            nextNode = entry.linkNext;

            return lastReturned = entry;
        }

        // Unused as of now but can be used for NavigableMap amongst other things
        @SuppressWarnings("unused")
        protected SplayedEntry<E> previousNode() {
            final SplayedEntry<E> entry = nextNode;

            if (nextNode == entries) {
                throw new NoSuchElementException();
            }
            if (expectedModCount != LinkedSplayMap.this.modCount) {
                throw new ConcurrentModificationException();
            }

            nextNode = nextNode.linkPrev;

            return lastReturned = entry;
        }

        @Override
        public void remove() {
            if (lastReturned == null) {
                throw new IllegalStateException();
            }
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }

            delete(lastReturned);

            expectedModCount = modCount;
            lastReturned = null;
        }
    }

    private class LinkedSplayMapEntryIterator extends LinkedSplayMapIterator<Entry<UnsignedInteger, E>> {

        public LinkedSplayMapEntryIterator(SplayedEntry<E> startAt) {
            super(startAt);
        }

        @Override
        public Entry<UnsignedInteger, E> next() {
            return nextNode();
        }
    }

    private class LinkedSplayMapKeyIterator extends LinkedSplayMapIterator<UnsignedInteger> {

        public LinkedSplayMapKeyIterator(SplayedEntry<E> startAt) {
            super(startAt);
        }

        @Override
        public UnsignedInteger next() {
            return nextNode().getKey();
        }
    }

    private class LinkedSplayMapValueIterator extends LinkedSplayMapIterator<E> {

        public LinkedSplayMapValueIterator(SplayedEntry<E> startAt) {
            super(startAt);
        }

        @Override
        public E next() {
            return nextNode().getValue();
        }
    }

    //----- Splay Map Collection types

    private final class LinkedSplayMapValues extends AbstractCollection<E> {

        @Override
        public Iterator<E> iterator() {
            return new LinkedSplayMapValueIterator(entries.linkNext);
        }

        @Override
        public int size() {
            return LinkedSplayMap.this.size;
        }

        @Override
        public boolean contains(Object o) {
            return LinkedSplayMap.this.containsValue(o);
        }

        @Override
        public boolean remove(Object target) {
            final SplayedEntry<E> root = LinkedSplayMap.this.entries;

            for (SplayedEntry<E> entry = root.linkNext; entry != root; entry = entry.linkNext) {
                if (entry.valueEquals(target)) {
                    delete(entry);
                    return true;
                }
            }
            return false;
        }

        @Override
        public void forEach(Consumer<? super E> action) {
            Objects.requireNonNull(action, "forEach action paremeter cannot be null");

            final int initialModCount = modCount;

            final SplayedEntry<E> root = LinkedSplayMap.this.entries;
            for (SplayedEntry<E> entry = root.linkNext; entry != entries; entry = entry.linkNext) {
                action.accept(entry.value);
            }

            if (modCount != initialModCount) {
                throw new ConcurrentModificationException();
            }
        }

        @Override
        public void clear() {
            LinkedSplayMap.this.clear();
        }
    }

    private final class LinkedSplayMapKeySet extends AbstractSet<UnsignedInteger> {

        @Override
        public Iterator<UnsignedInteger> iterator() {
            return new LinkedSplayMapKeyIterator(entries.linkNext);
        }

        @Override
        public int size() {
            return LinkedSplayMap.this.size;
        }

        @Override
        public boolean contains(Object o) {
            return LinkedSplayMap.this.containsKey(o);
        }

        @Override
        public boolean remove(Object target) {
            final SplayedEntry<E> root = LinkedSplayMap.this.entries;

            for (SplayedEntry<E> entry = root.linkNext; entry != root; entry = entry.linkNext) {
                if (entry.keyEquals(target)) {
                    delete(entry);
                    return true;
                }
            }
            return false;
        }

        @Override
        public void forEach(Consumer<? super UnsignedInteger> action) {
            Objects.requireNonNull(action, "forEach action paremeter cannot be null");

            final int initialModCount = modCount;

            final SplayedEntry<E> root = LinkedSplayMap.this.entries;
            for (SplayedEntry<E> entry = root.linkNext; entry != entries; entry = entry.linkNext) {
                action.accept(entry.getKey());
            }

            if (modCount != initialModCount) {
                throw new ConcurrentModificationException();
            }
        }

        @Override
        public void clear() {
            LinkedSplayMap.this.clear();
        }
    }

    private final class LinkedSplayMapEntrySet extends AbstractSet<Entry<UnsignedInteger, E>> {

        @Override
        public Iterator<Entry<UnsignedInteger, E>> iterator() {
            return new LinkedSplayMapEntryIterator(entries.linkNext);
        }

        @Override
        public int size() {
            return LinkedSplayMap.this.size;
        }

        @Override
        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }

            final SplayedEntry<E> root = LinkedSplayMap.this.entries;

            for (SplayedEntry<E> entry = root.linkNext; entry != root; entry = entry.linkNext) {
                if (entry.equals(o)) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public void forEach(Consumer<? super Entry<UnsignedInteger, E>> action) {
            Objects.requireNonNull(action, "forEach action paremeter cannot be null");

            final int initialModCount = modCount;

            final SplayedEntry<E> root = LinkedSplayMap.this.entries;

            for (SplayedEntry<E> entry = root.linkNext; entry != entries; entry = entry.linkNext) {
                action.accept(entry);
            }

            if (modCount != initialModCount) {
                throw new ConcurrentModificationException();
            }
        }

        @Override
        public boolean remove(Object target) {
            if (!(target instanceof Entry)) {
                throw new IllegalArgumentException("value provided is not an Entry type.");
            }

            final SplayedEntry<E> root = LinkedSplayMap.this.entries;

            for (SplayedEntry<E> entry = root.linkNext; entry != root; entry = entry.linkNext) {
                if (entry.equals(target)) {
                    delete(entry);
                    return true;
                }
            }
            return false;
        }

        @Override
        public void clear() {
            LinkedSplayMap.this.clear();
        }
    }
}
