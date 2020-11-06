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
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.apache.qpid.protonj2.types.UnsignedInteger;

/**
 * Map class that is implemented using a Splay Tree and uses primitive integers as the keys
 * for the specified value type.
 *
 * The splay tree is a specialized form of a binary search tree that is self balancing and
 * provides faster access in general to frequently used items.  The splay tree serves well
 * as an LRU cache of sorts where 80 percent of the accessed elements comes from 20 percent
 * of the overall load in the {@link Map}.  The best case access time is generally O(long n)
 * however it can be Theta(n) in a very worst case scenario.
 *
 * @param <E> The type stored in the map entries
 */
public final class SplayMap<E> implements NavigableMap<UnsignedInteger, E> {

    private static final UnsignedComparator COMPARATOR = new UnsignedComparator();

    /**
     * Cache of reusable entries filled when values are removed from the Map
     */
    private final RingQueue<SplayedEntry<E>> entryPool = new RingQueue<>(64);

    /**
     * Root node which can be null if the tree has no elements (size == 0)
     */
    private SplayedEntry<E> root;

    /**
     * Current size of the splayed map tree.
     */
    private int size;

    /**
     * Modification tracker for use in detecting concurrent modifications
     */
    private int modCount;

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Gets the value of the element stored in the {@link Map} with the key (treated as an
     * unsigned integer for comparison.
     *
     * As a side effect of calling this method the tree that comprises the Map can be modified
     * to bring up the found key or the last accessed key if the key given is not in the {@link Map}.
     * For entries at the root of the tree that match the given search key the method returns
     * immediately without modifying the {@link Map}.
     *
     * @param key
     *      the integer key value to search for in the {@link SplayMap}.
     *
     * @return the value stored for the given key if found or null if not in the {@link Map}.
     */
    public E get(int key) {
        if (root == null) {
            return null;
        } else if (root.key == key) {
            return root.value;
        } else {
            root = splay(root, key);

            if (root.key == key) {
                return root.value;
            } else {
                return null;
            }
        }
    }

    public E put(int key, E value) {
        E oldValue = null;

        if (root == null) {
            root = entryPool.poll(SplayMap::createEmtry).initialize(key, value);
        } else {
            root = splay(root, key);
            if (root.key == key) {
                oldValue = root.value;
                root.value = value;
                size--;
            } else {
                final SplayedEntry<E> node = entryPool.poll(SplayMap::createEmtry).initialize(key, value);

                if (compare(key, root.key) < 0) {
                    shiftRootRightOf(node);
                } else {
                    shiftRootLeftOf(node);
                }
            }
        }

        size++;
        modCount++;

        return oldValue;
    }

    @Override
    public E putIfAbsent(UnsignedInteger key, E value) {
        return putIfAbsent(key.intValue(), value);
    }

    public E putIfAbsent(int key, E value) {
        if (root == null) {
            root = entryPool.poll(SplayMap::createEmtry).initialize(key, value);
        } else {
            root = splay(root, key);
            if (root.key == key) {
                return root.value;
            } else {
                final SplayedEntry<E> node = entryPool.poll(SplayMap::createEmtry).initialize(key, value);

                if (compare(key, root.key) < 0) {
                    shiftRootRightOf(node);
                } else {
                    shiftRootLeftOf(node);
                }
            }
        }

        size++;
        modCount++;

        return null;
    }

    private void shiftRootRightOf(SplayedEntry<E> newRoot) {
        newRoot.right = root;
        newRoot.left = root.left;
        if (newRoot.left != null) {
            newRoot.left.parent = newRoot;
        }
        root.left = null;
        root.parent = newRoot;
        root = newRoot;
    }

    private void shiftRootLeftOf(SplayedEntry<E> newRoot) {
        newRoot.left = root;
        newRoot.right = root.right;
        if (newRoot.right != null) {
            newRoot.right.parent = newRoot;
        }
        root.right = null;
        root.parent = newRoot;
        root = newRoot;
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
        return get(Number.class.cast(key).intValue());
    }

    @Override
    public E remove(Object key) {
        return remove(Number.class.cast(key).intValue());
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
    private Set<Entry<UnsignedInteger, E>> entrySet;

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
        if (entrySet == null) {
            entrySet = new SplayMapEntrySet();
        }
        return entrySet;
    }

    @Override
    public void forEach(BiConsumer<? super UnsignedInteger, ? super E> action) {
        Objects.requireNonNull(action);

        for (SplayedEntry<E> entry = firstEntry(root); entry != null; entry = successor(entry)) {
            action.accept(entry.getKey(), entry.getValue());
        }
    }

    /**
     * A specialized forEach implementation that accepts a {@link Consumer} function that will
     * be called for each value in the {@link SplayMap}.  This method can save overhead as it does not
     * need to box the primitive key values into an object for the call to the provided function.
     * Unless otherwise specified by the implementing class, actions are performed in the order of entry
     * set iteration (if an iteration order is specified.)
     *
     * @param action
     *      The action to be performed for each of the values in the {@link SplayMap}.
     */
    public void forEach(Consumer<? super E> action) {
        Objects.requireNonNull(action);

        for (SplayedEntry<E> entry = firstEntry(root); entry != null; entry = successor(entry)) {
            action.accept(entry.getValue());
        }
    }

    @Override
    public void replaceAll(BiFunction<? super UnsignedInteger, ? super E, ? extends E> function) {
        Objects.requireNonNull(function, "The replacement function parameter cannot be null");

        final int initialModCount = modCount;

        for (SplayedEntry<E> entry = firstEntry(root); entry != null; entry = successor(entry)) {
            entry.value = function.apply(entry.getKey(), entry.value);
        }

        if (modCount != initialModCount) {
            throw new ConcurrentModificationException();
        }
    }

    //----- Internal Implementation

    /*
     * Rotations of tree elements form the basis of search and balance operations
     * within the tree during put, get and remove type operations.
     *
     *       y                                     x
     *      / \     Zig (Right Rotation)          /  \
     *     x   T3   – - – - – - – - - ->         T1   y
     *    / \       < - - - - - - - - -              / \
     *   T1  T2     Zag (Left Rotation)            T2   T3
     *
     */

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

    /*
     * The requested key if present is brought to the root of the tree.  If it is not
     * present then the last accessed element (nearest match) will be brought to the root
     * as it is likely it will be the next accessed or one of the neighboring nodes which
     * reduces the search time for that cluster.
     */
    private SplayedEntry<E> splay(SplayedEntry<E> root, int key) {
        if (root == null || root.key == key) {
            return root;
        }

        SplayedEntry<E> lessThanKeyRoot = null;
        SplayedEntry<E> lessThanKeyNode = null;
        SplayedEntry<E> greaterThanKeyRoot = null;
        SplayedEntry<E> greaterThanKeyNode = null;

        while (true) {
            if (compare(key, root.key) < 0) {
                // Entry must be to the left of the current node so we bring that up
                // and then work from there to see if we can find the key
                if (root.left != null && compare(key, root.left.key) < 0) {
                    root = rightRotate(root);
                }

                // Is there nowhere else to go, if so we are done.
                if (root.left == null) {
                    break;
                }

                // Haven't found it yet but we now know the current element is greater
                // than the element we are looking for so it goes to the right tree.
                if (greaterThanKeyRoot == null) {
                    greaterThanKeyRoot = greaterThanKeyNode = root;
                } else {
                    greaterThanKeyNode.left = root;
                    greaterThanKeyNode.left.parent = greaterThanKeyNode;
                    greaterThanKeyNode = root;
                }

                root = root.left;
                root.parent = null;
            } else if (compare(key, root.key) > 0) {
                // Entry must be to the right of the current node so we bring that up
                // and then work from there to see if we can find the key
                if (root.right != null && compare(key, root.right.key) > 0) {
                    root = leftRotate(root);
                }

                // Is there nowhere else to go, if so we are done.
                if (root.right == null) {
                    break;
                }

                // Haven't found it yet but we now know the current element is less
                // than the element we are looking for so it goes to the left tree.
                if (lessThanKeyRoot == null) {
                    lessThanKeyRoot = lessThanKeyNode = root;
                } else {
                    lessThanKeyNode.right = root;
                    lessThanKeyNode.right.parent = lessThanKeyNode;
                    lessThanKeyNode = root;
                }

                root = root.right;
                root.parent = null;
            } else {
                break; // Found it
            }
        }

        // Reassemble the tree from the left, right and middle the assembled nodes in the
        // left and right should have their last element either nulled out or linked to the
        // remaining items middle tree
        if (lessThanKeyRoot == null) {
            lessThanKeyRoot = root.left;
        } else {
            lessThanKeyNode.right = root.left;
            if (lessThanKeyNode.right != null) {
                lessThanKeyNode.right.parent = lessThanKeyNode;
            }
        }

        if (greaterThanKeyRoot == null) {
            greaterThanKeyRoot = root.right;
        } else {
            greaterThanKeyNode.left = root.right;
            if (greaterThanKeyNode.left != null) {
                greaterThanKeyNode.left.parent = greaterThanKeyNode;
            }
        }

        // The found or last accessed element is now rooted to the splayed
        // left and right trees and returned as the new tree.
        root.left = lessThanKeyRoot;
        if (root.left != null) {
            root.left.parent = root;
        }
        root.right = greaterThanKeyRoot;
        if (root.right != null) {
            root.right.parent = root;
        }

        return root;
    }

    private void delete(SplayedEntry<E> node) {
        final SplayedEntry<E> grandparent = node.parent;
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

        // Clear node before moving to cache
        node.left = node.right = node.parent = null;
        entryPool.offer(node);

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

    private static <E> SplayedEntry<E> createEmtry() {
        return new SplayedEntry<>();
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
            final SplayedEntry<E> entry = nextNode;

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
            final SplayedEntry<E> entry = nextNode;

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
            if (!(o instanceof Map.Entry) || SplayMap.this.root == null) {
                return false;
            }

            for (SplayedEntry<E> e = firstEntry(root); e != null; e = successor(e)) {
                if (e.equals(o)) {
                    return true;
                }
            }

            return false;
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

        public SplayedEntry() {
            initialize(key, value);
        }

        public SplayedEntry<E> initialize(int key, E value) {
            this.key = key;
            this.value = value;

            return this;
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
            return key ^ (value == null ? 0 : value.hashCode());
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
        return export(lowerEntry(key.intValue()));
    }

    @Override
    public UnsignedInteger lowerKey(UnsignedInteger key) {
        final SplayedEntry<E> result = lowerEntry(key.intValue());

        return result == null ? null : result.getKey();
    }

    private SplayedEntry<E> lowerEntry(int key) {
        root = splay(root, key);

        while (root != null) {
            if (compare(root.getIntKey(), key) >= 0) {
                root = predecessor(root);
            } else {
                break;
            }
        }

        return root;
    }

    @Override
    public ImmutableSplayMapEntry higherEntry(UnsignedInteger key) {
        return export(higherEntry(key.intValue()));
    }

    @Override
    public UnsignedInteger higherKey(UnsignedInteger key) {
        final SplayedEntry<E> result = higherEntry(key.intValue());

        return result == null ? null : result.getKey();
    }

    private SplayedEntry<E> higherEntry(int key) {
        root = splay(root, key);

        while (root != null) {
            if (compare(root.getIntKey(), key) <= 0) {
                root = successor(root);
            } else {
                break;
            }
        }

        return root;
    }

    @Override
    public ImmutableSplayMapEntry floorEntry(UnsignedInteger key) {
        return export(floorEntry(key.intValue()));
    }

    @Override
    public UnsignedInteger floorKey(UnsignedInteger key) {
        final SplayedEntry<E> result = floorEntry(key.intValue());

        return result == null ? null : result.getKey();
    }

    private SplayedEntry<E> floorEntry(int key) {
        root = splay(root, key);

        while (root != null) {
            if (compare(root.getIntKey(), key) > 0) {
                root = predecessor(root);
            } else {
                break;
            }
        }

        return root;
    }

    @Override
    public ImmutableSplayMapEntry ceilingEntry(UnsignedInteger key) {
        return export(ceilingEntry(key.intValue()));
    }

    @Override
    public UnsignedInteger ceilingKey(UnsignedInteger key) {
        final SplayedEntry<E> result = ceilingEntry(key.intValue());

        return result == null ? null : result.getKey();
    }

    private SplayedEntry<E> ceilingEntry(int key) {
        root = splay(root, key);

        while (root != null) {
            if (compare(root.getIntKey(), key) < 0) {
                root = successor(root);
            } else {
                break;
            }
        }

        return root;
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
