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
package org.apache.qpid.proton4j.engine.util;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;

/**
 * Map class that is implemented using a Splay Tree and uses primitive integers as the keys
 * for the specified value type.
 *
 * @param <E> The type stored in the map entries
 */
public class SplayMap<E> implements NavigableMap<UnsignedInteger, E> {

    private static final UnsignedComparator COMPARATOR = new UnsignedComparator();

    private int size;

    private SplayedEntry<E> root;

    public int modCount;

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    public E get(int key) {
        if (root == null) {
            return null;
        }

        root = splay(root, key);

        if (root.key == key) {
            return root.value;
        }

        return null;
    }

    public E put(int key, E value) {
        E oldValue = null;

        if (root == null) {
            root = new SplayedEntry<>(key, value);
        } else {
            root = splay(root, key);
            if (root.key == key) {
                oldValue = root.value;
                root.value = value;
            } else {
                SplayedEntry<E> node = new SplayedEntry<>(key, value);
                if (compare(key, root.key) < 0) {
                    node.right = root;
                    node.left = root.left;
                    if (node.left != null) {
                        node.left.parent = node;
                    }
                    root.left = null;
                    root.parent = node;
                } else {
                    node.left = root;
                    node.right = root.right;
                    if (node.right != null) {
                        node.right.parent = node;
                    }
                    root.right = null;
                    root.parent = node;
                }

                root = node;
            }
        }

        if (oldValue == null) {
            size++;
        }

        modCount++;

        return oldValue;
    }

    public E remove(UnsignedInteger key) {
        return remove(key.intValue());
    }

    public E remove(int key) {
        if (root == null) {
            return null;
        }

        root = splay(root, key);
        if (root.key != key) {
            return null;
        }

        final E removed = root.value;

        // We splayed on the key and matched it so the root
        // will now be the node matching that key.
        delete(root);

        return removed;
    }

    public boolean containsKey(int key) {
        if (root == null) {
            return false;
        }

        root = splay(root, key);
        if (root.key == key) {
            return true;
        }

        return false;
    }

    //----- Map interface implementation

    @Override
    public E put(UnsignedInteger key, E value) {
        return put(key.intValue(), value);
    }

    @Override
    public E get(Object key) {
        Number uintKey = (Number) key;
        return get(uintKey.intValue());
    }

    @Override
    public E remove(Object key) {
        return remove(Number.class.cast(key));
    }

    @Override
    public boolean containsKey(Object key) {
        Number numericKey = (Number) key;
        return containsKey(numericKey.intValue());
    }

    @Override
    public void clear() {
        root = null;
        size = 0;
    }

    @Override
    public void putAll(Map<? extends UnsignedInteger, ? extends E> source) {
        for (Entry<? extends UnsignedInteger, ? extends E> entry : source.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public boolean containsValue(Object value) {
        for (SplayedEntry<E> entry = firstEntry(root); entry != null; entry = successor(entry)) {
            if (entry.valueEquals(value)) {
                return true;
            }
        }

        return false;
    }

    // Once requested we will create an store a single instance to a collection
    // with no state for each of the key, values and entries types.  Since the
    // types are stateless the trivial race on create is not important to the
    // eventual outcome of having a cached instance.

    private Set<UnsignedInteger> keySet;
    private Collection<E> values;
    private Set<Entry<UnsignedInteger, E>> entries;

    @Override
    public Set<UnsignedInteger> keySet() {
        if (keySet == null) {
            keySet = new SplayMapKeySet();
        }
        return keySet;
    }

    @Override
    public Collection<E> values() {
        if (values == null) {
            values = new SplayMapValues();
        }
        return values;
    }

    @Override
    public Set<Entry<UnsignedInteger, E>> entrySet() {
        if (entries == null) {
            entries = new SplayMapEntrySet();
        }
        return entries;
    }

    //----- Internal Implementation

    private SplayedEntry<E> rightRotate(SplayedEntry<E> node) {
        SplayedEntry<E> rotated = node.left;
        node.left = rotated.right;
        rotated.right = node;

        // Reset the parent values for adjusted nodes.
        rotated.parent = node.parent;
        node.parent = rotated;
        if (node.left != null) {
            node.left.parent = node;
        }

        return rotated;
    }

    private SplayedEntry<E> leftRotate(SplayedEntry<E> node) {
        SplayedEntry<E> rotated = node.right;
        node.right = rotated.left;
        rotated.left = node;

        // Reset the parent values for adjusted nodes.
        rotated.parent = node.parent;
        node.parent = rotated;
        if (node.right != null) {
            node.right.parent = node;
        }

        return rotated;
    }

    private SplayedEntry<E> splay(SplayedEntry<E> root, int key) {
        if (root == null || root.key == key) {
            return root;
        }

        if (compare(root.key, key) > 0) {
            if (root.left == null) {
                return root;
            }

            if (compare(root.left.key, key) > 0) {
                root.left.left = splay(root.left.left, key);

                root = rightRotate(root);
            } else if (compare(root.left.key, key) > 0) {
                root.left.right = splay(root.left.right, key);

                if (root.left.right != null) {
                    root.left = leftRotate(root);
                }
            }

            return root.left == null ? root : rightRotate(root);
        } else {
            if (root.right == null) {
                return root;
            }

            if (compare(root.right.key, key) > 0) {
                root.right.left = splay(root.right.left, key);

                if (root.right.left != null) {
                    root.right = rightRotate(root.right);
                }
            } else if (compare(root.right.key, key) < 0) {
                root.right.right = splay(root.right.right, key);
                root = leftRotate(root);
            }

            return root.right == null ? root : leftRotate(root);
        }
    }

    private void delete(SplayedEntry<E> node) {
        SplayedEntry<E> grandparent = node.parent;
        SplayedEntry<E> replacement = node.right;

        if (node.left != null) {
            replacement = splay(node.left, node.key);
            replacement.right = node.right;
        }

        if (replacement != null) {
            replacement.parent = grandparent;
        }

        if (grandparent != null) {
            if (grandparent.left == node) {
                grandparent.left = replacement;
            } else {
                grandparent.right = replacement;
            }
        } else {
            root = replacement;
        }

        size--;
        modCount++;
    }

    private SplayedEntry<E> firstEntry(SplayedEntry<E> node) {
        SplayedEntry<E> firstEntry = node;
        if (firstEntry != null) {
            while (firstEntry.left != null) {
                firstEntry = firstEntry.left;
            }
        }

        return firstEntry;
    }

    private SplayedEntry<E> lastEntry(SplayedEntry<E> node) {
        SplayedEntry<E> lastEntry = node;
        if (lastEntry != null) {
            while (lastEntry.right != null) {
                lastEntry = lastEntry.right;
            }
        }

        return lastEntry;
    }

    private SplayedEntry<E> successor(SplayedEntry<E> node) {
        if (node == null) {
            return null;
        } else if (node.right != null) {
            // Walk to bottom of tree from this node's right child.
            SplayedEntry<E> result = node.right;
            while (result.left != null) {
                result = result.left;
            }

            return result;
        } else {
            SplayedEntry<E> parent = node.parent;
            SplayedEntry<E> child = node;
            while (parent != null && child == parent.right) {
                child = parent;
                parent = parent.parent;
            }

            return parent;
        }
    }

    private SplayedEntry<E> predecessor(SplayedEntry<E> node) {
        if (node == null) {
            return null;
        } else if (node.left != null) {
            // Walk to bottom of tree from this node's left child.
            SplayedEntry<E> result = node.left;
            while (result.right != null) {
                result = result.right;
            }

            return result;
        } else {
            SplayedEntry<E> parent = node.parent;
            SplayedEntry<E> child = node;
            while (parent != null && child == parent.left) {
                child = parent;
                parent = parent.parent;
            }

            return parent;
        }
    }

    private static int compare(int lhs, int rhs) {
        return Integer.compareUnsigned(lhs, rhs);
    }

    //----- Map Iterator implementation for EntrySet, KeySet and Values collections

    // Base class iterator that can be used for the collections returned from the Map
    private abstract class SplayMapIterator<T> implements Iterator<T> {

        private SplayedEntry<E> nextNode;
        private SplayedEntry<E> lastReturned;

        private int expectedModCount;

        public SplayMapIterator(SplayedEntry<E> startAt) {
            this.nextNode = startAt;
            this.expectedModCount = SplayMap.this.modCount;
        }

        @Override
        public boolean hasNext() {
            return nextNode != null;
        }

        protected SplayedEntry<E> nextNode() {
            SplayedEntry<E> entry = nextNode;
            if (nextNode == null) {
                throw new NoSuchElementException();
            }
            if (expectedModCount != SplayMap.this.modCount) {
                throw new ConcurrentModificationException();
            }

            nextNode = successor(nextNode);
            lastReturned = entry;

            return lastReturned;
        }

        // Unused as of now but can be used for NavigableMap amongst other things
        @SuppressWarnings("unused")
        protected SplayedEntry<E> previousNode() {
            SplayedEntry<E> entry = nextNode;
            if (nextNode == null) {
                throw new NoSuchElementException();
            }
            if (expectedModCount != SplayMap.this.modCount) {
                throw new ConcurrentModificationException();
            }

            nextNode = predecessor(nextNode);
            lastReturned = entry;

            return lastReturned;
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

    private class SplayMapEntryIterator extends SplayMapIterator<Entry<UnsignedInteger, E>> {

        public SplayMapEntryIterator(SplayedEntry<E> startAt) {
            super(startAt);
        }

        @Override
        public Entry<UnsignedInteger, E> next() {
            return nextNode();
        }
    }

    private class SplayMapKeyIterator extends SplayMapIterator<UnsignedInteger> {

        public SplayMapKeyIterator(SplayedEntry<E> startAt) {
            super(startAt);
        }

        @Override
        public UnsignedInteger next() {
            return nextNode().getKey();
        }
    }

    private class SplayMapValueIterator extends SplayMapIterator<E> {

        public SplayMapValueIterator(SplayedEntry<E> startAt) {
            super(startAt);
        }

        @Override
        public E next() {
            return nextNode().getValue();
        }
    }

    //----- Splay Map Collection types

    private final class SplayMapValues extends AbstractCollection<E> {

        @Override
        public Iterator<E> iterator() {
            return new SplayMapValueIterator(firstEntry(root));
        }

        @Override
        public int size() {
            return SplayMap.this.size;
        }

        @Override
        public boolean contains(Object o) {
            return SplayMap.this.containsValue(o);
        }

        @Override
        public boolean remove(Object target) {
            for (SplayedEntry<E> e = firstEntry(root); e != null; e = successor(e)) {
                if (e.valueEquals(target)) {
                    delete(e);
                    return true;
                }
            }
            return false;
        }

        @Override
        public void clear() {
            SplayMap.this.clear();
        }
    }

    private final class SplayMapKeySet extends AbstractSet<UnsignedInteger> {

        @Override
        public Iterator<UnsignedInteger> iterator() {
            return new SplayMapKeyIterator(firstEntry(root));
        }

        @Override
        public int size() {
            return SplayMap.this.size;
        }

        @Override
        public boolean contains(Object o) {
            return SplayMap.this.containsKey(o);
        }

        @Override
        public boolean remove(Object target) {
            for (SplayedEntry<E> e = firstEntry(root); e != null; e = successor(e)) {
                if (e.keyEquals(target)) {
                    delete(e);
                    return true;
                }
            }
            return false;
        }

        @Override
        public void clear() {
            SplayMap.this.clear();
        }
    }

    private final class SplayMapEntrySet extends AbstractSet<Entry<UnsignedInteger, E>> {

        @Override
        public Iterator<Entry<UnsignedInteger, E>> iterator() {
            return new SplayMapEntryIterator(firstEntry(root));
        }

        @Override
        public int size() {
            return SplayMap.this.size;
        }

        @Override
        public boolean contains(Object o) {
            return SplayMap.this.containsKey(o);
        }

        @Override
        public boolean remove(Object target) {
            if (!(target instanceof Entry)) {
                throw new IllegalArgumentException("value provided is not an Entry type.");
            }

            for (SplayedEntry<E> e = firstEntry(root); e != null; e = successor(e)) {
                if (e.equals(target)) {
                    delete(e);
                    return true;
                }
            }
            return false;
        }

        @Override
        public void clear() {
            SplayMap.this.clear();
        }
    }

    //----- Map Entry node for the Splay Map

    private static final class SplayedEntry<E> implements Map.Entry<UnsignedInteger, E>{

        SplayedEntry<E> left;
        SplayedEntry<E> right;
        SplayedEntry<E> parent;

        int key;
        E value;

        public SplayedEntry(int key, E value) {
            this.key = key;
            this.value = value;
        }

        public int getIntKey() {
            return key;
        }

        @Override
        public UnsignedInteger getKey() {
            return UnsignedInteger.valueOf(key);
        }

        @Override
        public E getValue() {
            return value;
        }

        @Override
        public E setValue(E value) {
            E oldValue = this.value;
            this.value = value;
            return oldValue;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }

            Map.Entry<?,?> e = (Map.Entry<?,?>)o;

            return keyEquals(e.getKey()) && valueEquals(e.getValue());
        }

        @Override
        public int hashCode() {
            return key ^ (value==null ? 0 : value.hashCode());
        }

        @Override
        public String toString() {
            return "Node:{" + key + "," + value + "}";
        }

        boolean keyEquals(Object other) {
            if (!(other instanceof Number)) {
                return false;
            }

            return key == ((Number) other).intValue();
        }

        boolean valueEquals(Object other) {
            return value != null ? value.equals(other) : other == null;
        }
    }

    public class ImmutableSplayMapEntry implements Map.Entry<UnsignedInteger, E> {

        private final SplayedEntry<E> entry;

        public ImmutableSplayMapEntry(SplayedEntry<E> entry) {
            this.entry = entry;
        }

        @Override
        public UnsignedInteger getKey() {
            return entry.getKey();
        }

        public int getPrimitiveKey() {
            return entry.getIntKey();
        }

        @Override
        public E getValue() {
            return entry.getValue();
        }

        @Override
        public E setValue(E value) {
            throw new UnsupportedOperationException();
        }
    }

    private ImmutableSplayMapEntry export(SplayedEntry<E> entry) {
        return entry == null ? null : new ImmutableSplayMapEntry(entry);
    }

    //----- Unsigned Integer comparator for Navigable Maps

    private static final class UnsignedComparator implements Comparator<UnsignedInteger> {

        @Override
        public int compare(UnsignedInteger uint1, UnsignedInteger uint2) {
            return uint1.compareTo(uint2);
        }
    }

    //----- Navigable and Sorted Map implementation methods

    @Override
    public Comparator<? super UnsignedInteger> comparator() {
        return COMPARATOR;
    }

    @Override
    public UnsignedInteger firstKey() {
        return isEmpty() ? null : firstEntry(root).getKey();
    }

    @Override
    public UnsignedInteger lastKey() {
        return isEmpty() ? null : lastEntry(root).getKey();
    }

    @Override
    public ImmutableSplayMapEntry firstEntry() {
        return export(firstEntry(root));
    }

    @Override
    public ImmutableSplayMapEntry lastEntry() {
        return export(lastEntry(root));
    }

    @Override
    public ImmutableSplayMapEntry pollFirstEntry() {
        SplayedEntry<E> firstEntry = firstEntry(root);
        if (firstEntry != null) {
            delete(firstEntry);
        }
        return export(firstEntry);
    }

    @Override
    public ImmutableSplayMapEntry pollLastEntry() {
        SplayedEntry<E> lastEntry = lastEntry(root);
        if (lastEntry != null) {
            delete(lastEntry);
        }
        return export(lastEntry);
    }

    @Override
    public ImmutableSplayMapEntry lowerEntry(UnsignedInteger key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UnsignedInteger lowerKey(UnsignedInteger key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ImmutableSplayMapEntry floorEntry(UnsignedInteger key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UnsignedInteger floorKey(UnsignedInteger key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ImmutableSplayMapEntry ceilingEntry(UnsignedInteger key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UnsignedInteger ceilingKey(UnsignedInteger key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ImmutableSplayMapEntry higherEntry(UnsignedInteger key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UnsignedInteger higherKey(UnsignedInteger key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NavigableMap<UnsignedInteger, E> descendingMap() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NavigableSet<UnsignedInteger> navigableKeySet() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NavigableSet<UnsignedInteger> descendingKeySet() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NavigableMap<UnsignedInteger, E> subMap(UnsignedInteger fromKey, boolean fromInclusive, UnsignedInteger toKey, boolean toInclusive) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NavigableMap<UnsignedInteger, E> headMap(UnsignedInteger toKey, boolean inclusive) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NavigableMap<UnsignedInteger, E> tailMap(UnsignedInteger fromKey, boolean inclusive) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SortedMap<UnsignedInteger, E> subMap(UnsignedInteger fromKey, UnsignedInteger toKey) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SortedMap<UnsignedInteger, E> headMap(UnsignedInteger toKey) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SortedMap<UnsignedInteger, E> tailMap(UnsignedInteger fromKey) {
        // TODO Auto-generated method stub
        return null;
    }
}
