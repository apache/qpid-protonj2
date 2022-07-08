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
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
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
import java.util.SortedSet;
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
public class SplayMap<E> implements NavigableMap<UnsignedInteger, E> {

    protected static final Comparator<UnsignedInteger> COMPARATOR = new UnsignedComparator();
    protected static final Comparator<UnsignedInteger> REVERSE_COMPARATOR = Collections.reverseOrder(COMPARATOR);

    protected final RingQueue<SplayedEntry<E>> entryPool = new RingQueue<>(64);

    /**
     * Root node which can be null if the tree has no elements (size == 0)
     */
    protected SplayedEntry<E> root;

    /**
     * Current size of the splayed map tree.
     */
    protected int size;

    protected int modCount;

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
     * @param defaultValue
     *      the default value to return if the key is not stored in this {@link Map}.
     *
     * @return the value stored for the given key if found the default value if not in the {@link Map}.
     */
    public E getOrDefault(int key, E defaultValue) {
        E result = get(key);
        if (result == null && root != null && root.key == key) {
            return null;
        }

        return defaultValue;
    }

    /**
     * Puts the value into the in the {@link Map} at the entry specified by the given key (treated as an
     * unsigned integer for comparison.
     *
     * As a side effect of calling this method the tree that comprises the Map can be modified
     * to bring up the found key or the last accessed key if the key given is not in the {@link Map}.
     * For entries at the root of the tree that match the given search key the method returns
     * immediately without modifying the {@link Map}.
     *
     * @param key
     *      the integer key value to search for and or insert in the {@link SplayMap}.
     * @param value
     *      the value to assign to the entry accessed via the given key.
     *
     * @return the previous value stored for the given key if found or null if not in the {@link Map}.
     */
    public E put(int key, E value) {
        E oldValue = null;

        if (root == null) {
            root = entryPool.poll(SplayMap::createEntry).initialize(key, value);
        } else {
            root = splay(root, key);
            if (root.key == key) {
                oldValue = root.value;
                root.value = value;
            } else {
                final SplayedEntry<E> node = entryPool.poll(SplayMap::createEntry).initialize(key, value);

                if (compare(key, root.key) < 0) {
                    shiftRootRightOf(node);
                } else {
                    shiftRootLeftOf(node);
                }
            }
        }

        if (oldValue == null) {
            entryAdded(root);
            size++;
        }
        modCount++;

        return oldValue;
    }

    /**
     * If the specified key is not already associated with a value associates it with the given value and
     * returns null, otherwise returns the current value.
     *
     * As a side effect of calling this method the tree that comprises the Map can be modified
     * to bring up the found key or the last accessed key if the key given is not in the {@link Map}.
     * For entries at the root of the tree that match the given search key the method returns
     * immediately without modifying the {@link Map}.
     *
     * @param key
     *      the integer key value to search for and or insert in the {@link SplayMap}.
     * @param value
     *      the value to assign to the entry accessed via the given key.
     *
     * @return the previous value associated with the given key or null if none was present.
     */
    public E putIfAbsent(int key, E value) {
        if (root == null) {
            root = entryPool.poll(SplayMap::createEntry).initialize(key, value);
        } else {
            root = splay(root, key);
            if (root.key == key) {
                return root.value;
            } else {
                final SplayedEntry<E> node = entryPool.poll(SplayMap::createEntry).initialize(key, value);

                if (compare(key, root.key) < 0) {
                    shiftRootRightOf(node);
                } else {
                    shiftRootLeftOf(node);
                }
            }
        }

        entryAdded(root);
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

    /**
     * Removes the mapping for the {@link UnsignedInteger} key from this map if it is present
     * and returns the value to which this map previously associated the key, or null if the
     * map contained no mapping for the key.
     *
     * @param key
     * 		The {@link UnsignedInteger} key whose value will be removed from the {@link SplayMap}.
     *
     * @return the value that was removed if one was present in the {@link Map}.
     */
    public E remove(UnsignedInteger key) {
        return remove(key.intValue());
    }

    /**
     * Removes the mapping for the primitive <code>int</code> key from this map if it is present
     * and returns the value to which this map previously associated the key, or null if the
     * map contained no mapping for the key.  The integer value is treated as an unsigned int
     * internally.
     *
     * @param key
     * 		The {@link UnsignedInteger} key whose value will be removed from the {@link SplayMap}.
     *
     * @return the value that was removed if one was present in the {@link Map}.
     */
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

    /**
     * Searches the map using the given primitive integer key value which will be treated
     * internally as an unsigned value when comparing against keys in the mapping.
     *
     * @param key
     * 		The key which will be searched for in this mapping.
     *
     * @return <code>true</code> if the key mapping is found within this {@link Map}.
     */
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
    public E putIfAbsent(UnsignedInteger key, E value) {
        return putIfAbsent(key.intValue(), value);
    }

    @Override
    public E get(Object key) {
        return get(Number.class.cast(key).intValue());
    }

    @Override
    public E getOrDefault(Object key, E defaultValue) {
        return getOrDefault(Number.class.cast(key).intValue(), defaultValue);
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

    @Override
    public int hashCode() {
        int hash = 0;
        for (SplayedEntry<E> entry = firstEntry(root); entry != null; entry = successor(entry)) {
            hash += entry.hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Map)) {
            return false;
        }

        Map<?,?> m = (Map<?,?>) o;
        if (m.size() != size()) {
            return false;
        }

        try {
            for (SplayedEntry<E> entry = firstEntry(root); entry != null; entry = successor(entry)) {
                UnsignedInteger key = entry.getKey();
                E value = entry.getValue();
                if (value == null) {
                    if (!(m.get(key) == null && m.containsKey(key))) {
                        return false;
                    }
                } else {
                    if (!value.equals(m.get(key))) {
                        return false;
                    }
                }
            }
        } catch (ClassCastException | NullPointerException ignored) {
            return false;
        }

        return true;
    }

    // Once requested we will create an store a single instance to a collection
    // with no state for each of the key, values ,entries types and the descending
    // Map view. Since the types are stateless the trivial race on create is not
    // important to the eventual outcome of having a cached instance.

    protected NavigableSet<UnsignedInteger> keySet;
    protected Collection<E> values;
    protected Set<Entry<UnsignedInteger, E>> entrySet;
    protected NavigableMap<UnsignedInteger, E> descendingMapView;

    @Override
    public Set<UnsignedInteger> keySet() {
        return navigableKeySet();
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

    @Override
    public boolean remove(Object key, Object value) {
        Number numericKey = (Number) key;
        return remove(numericKey.intValue(), value);
    }

    /**
     * Removes the entry for the specified primitive int (treated as unsigned) key only if it is
     * currently mapped to the specified value in the {@link Map}.
     *
     * @param key
     * 		The key whose value will be removed if matched.
     * @param value
     * 		The value that must be contained in the mapping for the remove to be performed.
     *
     * @return <code>true</code> if an entry was removed from the {@link Map}
     */
    public boolean remove(int key, Object value) {
        root = splay(root, key);
        if (root == null || root.key != key || !Objects.equals(root.value, value)) {
            return false;
        } else {
            delete(root);
            return true;
        }
    }

    @Override
    public boolean replace(UnsignedInteger key, E oldValue, E newValue) {
        return replace(key.intValue(), oldValue, newValue);
    }

    /**
     * Replaces the entry for the specified primitive int (treated as unsigned) key only if it is
     * currently mapped to the specified value in the {@link Map} with the new value provided.
     *
     * @param key
     * 		The key whose value will be removed if matched.
     * @param oldValue
     * 		The old value that must be contained in the mapping for the replace to be performed.
     * @param newValue
     * 		The value that will replace the old value mapped to the given key if one existed..
     *
     * @return <code>true</code> if an entry was replaced in the {@link Map}
     */
    public boolean replace(int key, E oldValue, E newValue) {
        root = splay(root, key);
        if (root == null || root.key != key || !Objects.equals(root.value, oldValue)) {
            return false;
        } else {
            root.setValue(newValue);
            return true;
        }
    }

    @Override
    public E replace(UnsignedInteger key, E value) {
        return replace(key.intValue(), value);
    }

    /**
     * Replaces the entry for the specified primitive int (treated as unsigned) key only if it is
     * currently mapped to the a value in the {@link Map} with the new value provided.
     *
     * @param key
     * 		The key whose value will be removed if matched.
     * @param value
     * 		The value that will replace the old value mapped to the given key if one existed..
     *
     * @return <code>true</code> if an entry was replaced in the {@link Map}
     */
    public E replace(int key, E value) {
        root = splay(root, key);
        if (root == null || root.key != key || root.value == null) {
            return null;
        } else {
            return root.setValue(value);
        }
    }

    //----- Extension points

    protected void entryAdded(SplayedEntry<E> newEntry) {
        // Nothing to do in the base class implementation.
    }

    protected void entryDeleted(SplayedEntry<E> deletedEntry) {
        // Nothing to do in the base class implementation.
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

    private static <E> SplayedEntry<E> rightRotate(SplayedEntry<E> node) {
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

    private static <E> SplayedEntry<E> leftRotate(SplayedEntry<E> node) {
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
    private static <E> SplayedEntry<E> splay(SplayedEntry<E> root, int key) {
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

    protected void delete(SplayedEntry<E> node) {
        final SplayedEntry<E> grandparent = node.parent;
        SplayedEntry<E> replacement = node.right;

        if (node.left != null) {
            replacement = splay(node.left, node.key);
            replacement.right = node.right;
            if (replacement.right != null) {
                replacement.right.parent = replacement;
            }
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

        entryDeleted(node);

        size--;
        modCount++;
    }

    private SplayedEntry<E> findEntry(int key) {
        if (root == null) {
            return null;
        } else if (root.key == key) {
            return root;
        } else {
            root = splay(root, key);

            if (root.key == key) {
                return root;
            } else {
                return null;
            }
        }
    }

    private static <E> SplayedEntry<E> firstEntry(SplayedEntry<E> node) {
        SplayedEntry<E> firstEntry = node;
        if (firstEntry != null) {
            while (firstEntry.left != null) {
                firstEntry = firstEntry.left;
            }
        }

        return firstEntry;
    }

    private static <E> SplayedEntry<E> lastEntry(SplayedEntry<E> node) {
        SplayedEntry<E> lastEntry = node;
        if (lastEntry != null) {
            while (lastEntry.right != null) {
                lastEntry = lastEntry.right;
            }
        }

        return lastEntry;
    }

    private static <E> SplayedEntry<E> successor(SplayedEntry<E> node) {
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

    private static <E> SplayedEntry<E> predecessor(SplayedEntry<E> node) {
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

    /**
     * Unsigned comparison of two integer values
     *
     * @param lhs
     * 		the left hand side value for comparison.
     * @param rhs
     *      the right hand side value for comparison.
     *
     * @return a negative integer, zero, or a positive integer as the first argument is less than,
     * 		   equal to, or greater than the second.
     */
    private static int compare(int lhs, int rhs) {
        return Integer.compareUnsigned(lhs, rhs);
    }

    private static <E> SplayedEntry<E> createEntry() {
        return new SplayedEntry<>();
    }

    //----- Map Iterator implementation for EntrySet, KeySet and Values collections

    // Base class iterator that can be used for the collections returned from the Map
    private abstract class SplayMapIterator<T> implements Iterator<T> {

        protected SplayedEntry<E> nextNode;
        protected SplayedEntry<E> lastReturned;

        protected int expectedModCount;

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

    final class ReverseSplayMapKeyIterator extends SplayMapIterator<UnsignedInteger> {
        public ReverseSplayMapKeyIterator(SplayedEntry<E> startAt) {
            super(startAt);
        }

        @Override
        public UnsignedInteger next() {
            return previousNode().getKey();
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

    protected class SplayMapKeySet extends AbstractSet<UnsignedInteger> implements NavigableSet<UnsignedInteger> {

        @Override
        public Iterator<UnsignedInteger> iterator() {
            return new SplayMapKeyIterator(firstEntry(root));
        }

        @Override
        public int size() {
            return SplayMap.this.size;
        }

        @Override
        public boolean isEmpty() {
            return SplayMap.this.size == 0;
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
        public boolean retainAll(Collection<?> c) {
            Objects.requireNonNull(c);
            boolean modified = false;

            for (SplayedEntry<E> e = firstEntry(root); e != null;) {
                if (c.contains(e.getKey())) {
                    e = successor(e);
                } else {
                    final SplayedEntry<E> target = e;
                    e = successor(e);

                    delete(target);
                    modified = true;
                }
            }

            return modified;
        }

        @Override
        public Object[] toArray() {
            Object[] result = new Object[size()];
            int i = 0;

            for (SplayedEntry<E> e = firstEntry(root); e != null; e = successor(e), ++i) {
                result[i] = e.getKey();
            }

            return result;
        }

        @Override
        public void clear() {
            SplayMap.this.clear();
        }

        @Override
        public Comparator<? super UnsignedInteger> comparator() {
            return SplayMap.COMPARATOR;
        }

        @Override
        public UnsignedInteger first() {
            return firstKey();
        }

        @Override
        public UnsignedInteger last() {
            return lastKey();
        }

        @Override
        public UnsignedInteger lower(UnsignedInteger key) {
            return lowerKey(key);
        }

        @Override
        public UnsignedInteger floor(UnsignedInteger key) {
            return floorKey(key);
        }

        @Override
        public UnsignedInteger ceiling(UnsignedInteger key) {
            return ceilingKey(key);
        }

        @Override
        public UnsignedInteger higher(UnsignedInteger key) {
            return higherKey(key);
        }

        @Override
        public UnsignedInteger pollFirst() {
            Map.Entry<UnsignedInteger, ?> first = pollFirstEntry();
            return first == null ? null : first.getKey();
        }

        @Override
        public UnsignedInteger pollLast() {
            Map.Entry<UnsignedInteger, ?> first = pollLastEntry();
            return first == null ? null : first.getKey();
        }

        @Override
        public NavigableSet<UnsignedInteger> descendingSet() {
            return descendingMap().navigableKeySet();
        }

        @Override
        public Iterator<UnsignedInteger> descendingIterator() {
            return descendingMap().keySet().iterator();
        }

        @Override
        public NavigableSet<UnsignedInteger> subSet(UnsignedInteger fromElement, boolean fromInclusive, UnsignedInteger toElement, boolean toInclusive) {
            return new AscendingSubMap<>(SplayMap.this,
                                         false, fromElement.intValue(), fromInclusive,
                                         false, toElement.intValue(), toInclusive).navigableKeySet();
        }

        @Override
        public NavigableSet<UnsignedInteger> headSet(UnsignedInteger toElement, boolean inclusive) {
            return new AscendingSubMap<>(SplayMap.this,
                true, 0, true, false, toElement.intValue(), inclusive).navigableKeySet();
        }

        @Override
        public NavigableSet<UnsignedInteger> tailSet(UnsignedInteger fromElement, boolean inclusive) {
            return new AscendingSubMap<>(SplayMap.this,
                false, fromElement.intValue(), inclusive, true, 0, true).navigableKeySet();
        }

        @Override
        public SortedSet<UnsignedInteger> subSet(UnsignedInteger fromElement, UnsignedInteger toElement) {
            return subSet(fromElement, true, toElement, true);
        }

        @Override
        public SortedSet<UnsignedInteger> headSet(UnsignedInteger toElement) {
            return headSet(toElement, false);
        }

        @Override
        public SortedSet<UnsignedInteger> tailSet(UnsignedInteger fromElement) {
            return tailSet(fromElement, false);
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
            if ((o instanceof Map.Entry) && SplayMap.this.root != null) {
                for (SplayedEntry<E> e = firstEntry(root); e != null; e = successor(e)) {
                    if (e.equals(o)) {
                        return true;
                    }
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

    protected static final class SplayedEntry<E> implements Map.Entry<UnsignedInteger, E>{

        SplayedEntry<E> left;
        SplayedEntry<E> right;
        SplayedEntry<E> parent;

        int key;
        E value;

        // Insertion order chain used by LinkedSplayMap
        SplayedEntry<E> linkNext;
        SplayedEntry<E> linkPrev;

        public SplayedEntry() {
            initialize(key, value);
        }

        public SplayedEntry<E> initialize(int key, E value) {
            this.key = key;
            this.value = value;
            // Node is circular list to start.
            this.linkNext = this;
            this.linkPrev = this;

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

    /**
     * An immutable {@link Map} entry that can be used when exposing raw entry mappings
     * via the {@link Map} API.
     */
    public static class ImmutableSplayMapEntry<E> implements Map.Entry<UnsignedInteger, E> {

        private final SplayedEntry<E> entry;

        /**
         * Create a new immutable {@link Map} entry.
         *
         * @param entry
         * 		The inner {@link Map} entry that is wrapped.
         */
        public ImmutableSplayMapEntry(SplayedEntry<E> entry) {
            this.entry = entry;
        }

        @Override
        public UnsignedInteger getKey() {
            return entry.getKey();
        }

        /**
         * @return the primitive integer view of the unsigned key.
         */
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

    protected static <V> ImmutableSplayMapEntry<V> export(SplayedEntry<V> entry) {
        return entry == null ? null : new ImmutableSplayMapEntry<>(entry);
    }

    //----- Unsigned Integer comparator for Navigable Maps

    private static final class UnsignedComparator implements Comparator<UnsignedInteger> {

        @Override
        public int compare(UnsignedInteger uint1, UnsignedInteger uint2) {
            return uint1.compareTo(uint2);
        }
    }

    protected static Comparator<? super UnsignedInteger> reverseComparator() {
        return REVERSE_COMPARATOR;
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
    public ImmutableSplayMapEntry<E> firstEntry() {
        return export(firstEntry(root));
    }

    @Override
    public ImmutableSplayMapEntry<E> lastEntry() {
        return export(lastEntry(root));
    }

    @Override
    public ImmutableSplayMapEntry<E> pollFirstEntry() {
        SplayedEntry<E> firstEntry = firstEntry(root);
        if (firstEntry != null) {
            delete(firstEntry);
        }
        return export(firstEntry);
    }

    @Override
    public ImmutableSplayMapEntry<E> pollLastEntry() {
        SplayedEntry<E> lastEntry = lastEntry(root);
        if (lastEntry != null) {
            delete(lastEntry);
        }
        return export(lastEntry);
    }

    @Override
    public ImmutableSplayMapEntry<E> lowerEntry(UnsignedInteger key) {
        return export(splayToLowerEntry(key.intValue()));
    }

    @Override
    public UnsignedInteger lowerKey(UnsignedInteger key) {
        final SplayedEntry<E> result = splayToLowerEntry(key.intValue());

        return result == null ? null : result.getKey();
    }

    /**
     * Splay to a key-value mapping associated with the greatest key strictly less than the given
     * key, or null if there is no such key.
     *
     * @param key
     * 		The key whose next lower entry match is being queried.
     *
     * @return the next lower entry or null if no keys lower than the given value.
     */
    private SplayedEntry<E> splayToLowerEntry(int key) {
        root = splay(root, key);

        while (root != null) {
            if (compare(root.getIntKey(), key) >= 0) {
                SplayedEntry<E> pred = predecessor(root);
                if (pred != null) {
                    root = splay(root, pred.key);
                } else {
                    return null;
                }
            } else {
                break;
            }
        }

        return root;
    }

    @Override
    public ImmutableSplayMapEntry<E> higherEntry(UnsignedInteger key) {
        return export(splayToHigherEntry(key.intValue()));
    }

    @Override
    public UnsignedInteger higherKey(UnsignedInteger key) {
        final SplayedEntry<E> result = splayToHigherEntry(key.intValue());

        return result == null ? null : result.getKey();
    }

    /**
     * Splay to a key-value mapping associated with the least key strictly greater than the given
     * key, or null if there is no such key.
     *
     * @param key
     * 		The key whose next higher entry match is being queried.
     *
     * @return the next highest entry or null if no keys higher than the given value.
     */
    private SplayedEntry<E> splayToHigherEntry(int key) {
        root = splay(root, key);

        while (root != null) {
            if (compare(root.getIntKey(), key) <= 0) {
                SplayedEntry<E> succ = successor(root);
                if (succ != null) {
                    root = splay(root, succ.key);
                } else {
                    return null;
                }
            } else {
                break;
            }
        }

        return root;
    }

    @Override
    public ImmutableSplayMapEntry<E> floorEntry(UnsignedInteger key) {
        return export(splayToFloorEntry(key.intValue()));
    }

    @Override
    public UnsignedInteger floorKey(UnsignedInteger key) {
        final SplayedEntry<E> result = splayToFloorEntry(key.intValue());

        return result == null ? null : result.getKey();
    }

    /**
     * Splay to a key-value mapping associated with the greatest key less than or equal to
     * the given key, or null if there is no such key.
     *
     * @param key
     * 		The key whose floor entry match is being queried.
     *
     * @return the entry or next lowest entry or null if no keys less then or equal to the given value.
     */
    private SplayedEntry<E> splayToFloorEntry(int key) {
        root = splay(root, key);

        while (root != null) {
            if (compare(root.getIntKey(), key) > 0) {
                SplayedEntry<E> pred = predecessor(root);
                if (pred != null) {
                    root = splay(root, pred.key);
                } else {
                    return null;
                }
            } else {
                break;
            }
        }

        return root;
    }

    @Override
    public ImmutableSplayMapEntry<E> ceilingEntry(UnsignedInteger key) {
        return export(splayToCeilingEntry(key.intValue()));
    }

    @Override
    public UnsignedInteger ceilingKey(UnsignedInteger key) {
        final SplayedEntry<E> result = splayToCeilingEntry(key.intValue());

        return result == null ? null : result.getKey();
    }

    /**
     * Splay to a key-value mapping associated with the least key greater than or equal
     * to the given key, or null if there is no such key.
     *
     * @param key
     * 		The key whose ceiling entry match is being queried.
     *
     * @return the entry or next highest entry or null if no keys higher than or equal to the given value.
     */
    private SplayedEntry<E> splayToCeilingEntry(int key) {
        root = splay(root, key);

        while (root != null) {
            if (compare(root.getIntKey(), key) < 0) {
                SplayedEntry<E> succ = successor(root);
                if (succ != null) {
                    root = splay(root, succ.key);
                } else {
                    return null;
                }
            } else {
                break;
            }
        }

        return root;
    }

    @Override
    public NavigableMap<UnsignedInteger, E> descendingMap() {
        return (descendingMapView != null) ? descendingMapView :
               (descendingMapView = new DescendingSubMap<>(this,
                    true, 0, true,
                    true, UnsignedInteger.MAX_VALUE.intValue(), true));
    }

    @Override
    public NavigableSet<UnsignedInteger> navigableKeySet() {
        if (keySet == null) {
            keySet = new SplayMapKeySet();
        }
        return keySet;
    }

    @Override
    public NavigableSet<UnsignedInteger> descendingKeySet() {
        return descendingMap().navigableKeySet();
    }

    @Override
    public NavigableMap<UnsignedInteger, E> subMap(UnsignedInteger fromKey, boolean fromInclusive, UnsignedInteger toKey, boolean toInclusive) {
        return new AscendingSubMap<>(this, false, fromKey.intValue(), fromInclusive, false, toKey.intValue(), toInclusive);
    }

    @Override
    public NavigableMap<UnsignedInteger, E> headMap(UnsignedInteger toKey, boolean inclusive) {
        return new AscendingSubMap<>(this, true, 0, true, false, toKey.intValue(), inclusive);
    }

    @Override
    public NavigableMap<UnsignedInteger, E> tailMap(UnsignedInteger fromKey, boolean inclusive) {
        return new AscendingSubMap<>(this, false, fromKey.intValue(), inclusive, true, UnsignedInteger.MAX_VALUE.intValue(), true);
    }

    @Override
    public SortedMap<UnsignedInteger, E> subMap(UnsignedInteger fromKey, UnsignedInteger toKey) {
        return subMap(fromKey, true, toKey, false);
    }

    @Override
    public SortedMap<UnsignedInteger, E> headMap(UnsignedInteger toKey) {
        return headMap(toKey, false);
    }

    @Override
    public SortedMap<UnsignedInteger, E> tailMap(UnsignedInteger fromKey) {
        return tailMap(fromKey, true);
    }

    /**
     * Gets a key-value mapping associated with the given key or its successor.
     * This method does not splay the tree so it will not bring the result to the root.
     *
     * @param key
     * 		The key to search for in the mappings
     *
     * @return the entry that matches the search criteria or null if no valid match.
     */
    private SplayedEntry<E> getCeilingEntry(int key) {
        SplayedEntry<E> result = this.root;

        while (result != null) {
            final int comparison = SplayMap.compare(key, result.key);
            if (comparison < 0) {
                // search key is less than current go left to get a smaller value
                if (result.left != null) {
                    result = result.left;
                } else {
                    return result; // nothing smaller exists
                }
            } else if (comparison > 0) {
                // search key is greater than current go right to get a bigger one
                // or go back up to the root of this branch
                if (result.right != null) {
                    result = result.right;
                } else {
                    SplayedEntry<E> parent = result.parent;
                    SplayedEntry<E> current = result;
                    while (parent != null && current == parent.right) {
                        current = parent;
                        parent = parent.parent;
                    }
                    return parent;
                }
            } else {
                return result; // Found it.
            }
        }

        return null;
    }

    /**
     * Gets a key-value mapping associated with the given key or its predecessor.
     * This method does not splay the tree so it will not bring the result to the root.
     *
     * @param key
     * 		The key to search for in the mappings
     *
     * @return the entry that matches the search criteria or null if no valid match.
     */
    private SplayedEntry<E> getFloorEntry(int key) {
        SplayedEntry<E> result = this.root;

        while (result != null) {
            final int comparison = SplayMap.compare(key, result.key);
            if (comparison > 0) {
                // search key is greater than current go right to get a bigger value
                if (result.right != null) {
                    result = result.right;
                } else {
                    return result; // nothing bigger exists
                }
            } else if (comparison < 0) {
                // search key is less than current go left to get a smaller one
                // or go back up to the root of this branch
                if (result.left != null) {
                    result = result.left;
                } else {
                    SplayedEntry<E> parent = result.parent;
                    SplayedEntry<E> current = result;
                    while (parent != null && current == parent.left) {
                        current = parent;
                        parent = parent.parent;
                    }
                    return parent;
                }
            } else {
                return result; // Found it.
            }
        }

        return null;
    }

    /**
     * Gets a key-value mapping associated with the next entry higher than the given
     * key or null if no entries exists with a higher key value. This method does not
     * splay the tree so it will not bring the result to the root.
     *
     * @param key
     * 		The key to search for in the mappings
     *
     * @return the entry that matches the search criteria or null if no valid match.
     */
    private SplayedEntry<E> getHigherEntry(int key) {
        SplayedEntry<E> result = this.root;

        while (result != null) {
            final int comparison = SplayMap.compare(key, result.key);
            if (comparison < 0) {
                // search key is less than current go left to get a smaller value
                if (result.left != null) {
                    result = result.left;
                } else {
                    return result; // nothing smaller exists
                }
            } else {
                // search key is greater than current go right to get a bigger one
                // or go back up to the root of this branch
                if (result.right != null) {
                    result = result.right;
                } else {
                    SplayedEntry<E> parent = result.parent;
                    SplayedEntry<E> current = result;
                    while (parent != null && current == parent.right) {
                        current = parent;
                        parent = parent.parent;
                    }
                    return parent;
                }
            }
        }

        return null;
    }

    /**
     * Gets a key-value mapping associated with next smallest entry below the given key
     * or null if no smaller entries exist. This method does not splay the tree so it
     * will not bring the result to the root.
     *
     * @param key
     * 		The key to search for in the mappings
     *
     * @return the entry that matches the search criteria or null if no valid match.
     */
    private SplayedEntry<E> getLowerEntry(int key) {
        SplayedEntry<E> result = this.root;

        while (result != null) {
            final int comparison = SplayMap.compare(key, result.key);
            if (comparison > 0) {
                // search key is greater than current go right to get a bigger value
                if (result.right != null) {
                    result = result.right;
                } else {
                    return result; // nothing bigger exists
                }
            } else {
                // search key is less than current go left to get a smaller one
                // or go back up to the root of this branch
                if (result.left != null) {
                    result = result.left;
                } else {
                    SplayedEntry<E> parent = result.parent;
                    SplayedEntry<E> current = result;
                    while (parent != null && current == parent.left) {
                        current = parent;
                        parent = parent.parent;
                    }
                    return parent;
                }
            }
        }

        return null;
    }

    protected static class NavigableSubMapKeySet extends AbstractSet<UnsignedInteger> implements NavigableSet<UnsignedInteger> {

        private final NavigableSubMap<?> backingMap;

        public NavigableSubMapKeySet(NavigableSubMap<?> backingMap) {
            this.backingMap = backingMap;
        }

        @Override
        public Iterator<UnsignedInteger> iterator() {
            return backingMap.keyIterator();
        }

        @Override
        public Iterator<UnsignedInteger> descendingIterator() {
            return ((NavigableSubMap<?>)backingMap.descendingMap()).descendingKeyIterator();
        }

        @Override
        public int size() {
            return backingMap.size();
        }

        @Override
        public boolean isEmpty() {
            return backingMap.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return backingMap.containsKey(o);
        }

        @Override
        public boolean remove(Object target) {
            int oldSize = backingMap.size();
            backingMap.remove(target);
            return oldSize != backingMap.size();
        }

        @Override
        public void clear() {
            backingMap.clear();
        }

        @Override
        public Comparator<? super UnsignedInteger> comparator() {
            return backingMap.comparator();
        }

        @Override
        public UnsignedInteger first() {
            return backingMap.firstKey();
        }

        @Override
        public UnsignedInteger last() {
            return backingMap.lastKey();
        }

        @Override
        public UnsignedInteger lower(UnsignedInteger key) {
            return backingMap.lowerKey(key);
        }

        @Override
        public UnsignedInteger floor(UnsignedInteger key) {
            return backingMap.floorKey(key);
        }

        @Override
        public UnsignedInteger ceiling(UnsignedInteger key) {
            return backingMap.ceilingKey(key);
        }

        @Override
        public UnsignedInteger higher(UnsignedInteger key) {
            return backingMap.higherKey(key);
        }

        @Override
        public UnsignedInteger pollFirst() {
            Map.Entry<UnsignedInteger, ?> first = backingMap.pollFirstEntry();
            return first == null ? null : first.getKey();
        }

        @Override
        public UnsignedInteger pollLast() {
            Map.Entry<UnsignedInteger, ?> first = backingMap.pollLastEntry();
            return first == null ? null : first.getKey();
        }

        @Override
        public NavigableSet<UnsignedInteger> descendingSet() {
            return backingMap.descendingMap().navigableKeySet();
        }

        @Override
        public NavigableSet<UnsignedInteger> subSet(UnsignedInteger fromElement, boolean fromInclusive, UnsignedInteger toElement, boolean toInclusive) {
            return backingMap.subMap(fromElement, fromInclusive, toElement, toInclusive).navigableKeySet();
        }

        @Override
        public NavigableSet<UnsignedInteger> headSet(UnsignedInteger toElement, boolean inclusive) {
            return backingMap.headMap(toElement, inclusive).navigableKeySet();
        }

        @Override
        public NavigableSet<UnsignedInteger> tailSet(UnsignedInteger fromElement, boolean inclusive) {
            return backingMap.tailMap(fromElement, inclusive).navigableKeySet();
        }

        @Override
        public SortedSet<UnsignedInteger> subSet(UnsignedInteger fromElement, UnsignedInteger toElement) {
            return subSet(fromElement, true, toElement, true);
        }

        @Override
        public SortedSet<UnsignedInteger> headSet(UnsignedInteger toElement) {
            return headSet(toElement, false);
        }

        @Override
        public SortedSet<UnsignedInteger> tailSet(UnsignedInteger fromElement) {
            return tailSet(fromElement, false);
        }
    }

    /**
     * Utility base class for the {@link SplayMap} that allows access to a sub region of the
     * backing map.
     * <p>
     * If the from start directive is <code>true</code> then the map ignores the start key
     * and the start key inclusive directive and assigns the start key as zero and the
     * start key inclusive value to true.
     * <p>
     * If the to end directive is <code>true</code> then the map ignores the end key
     * and the end key inclusive directive and assigns the end key to {@link UnsignedInteger#MAX_VALUE}
     * and the end key inclusive flag to true.
     *
     * @param <E> The value type for this {@link NavigableSubMap}
     */
    private abstract static class NavigableSubMap<E> extends AbstractMap<UnsignedInteger, E> implements NavigableMap<UnsignedInteger, E> {

        protected final SplayMap<E> backingMap;

        final int startKey;
        final int endKey;
        final boolean fromStart;
        final boolean toEnd;
        final boolean startInclusive;
        final boolean endInclusive;

        private transient NavigableSubMapKeySet navigableSubMapKeySet;

        NavigableSubMap(final SplayMap<E> map,
                        final boolean fromStart, final int start, final boolean startInclusive,
                        final boolean toEnd, final int end, final boolean endInclusive) {

            if (SplayMap.compare(start, end) > 0) {
                throw new IllegalArgumentException("The start key cannot be greater than the end key");
            }

            this.backingMap = map;
            this.fromStart = fromStart;
            this.toEnd = toEnd;
            this.startKey = fromStart ? 0 : start;
            this.endKey = toEnd ? UnsignedInteger.MAX_VALUE.intValue() : end;
            this.startInclusive = fromStart ? true : startInclusive;
            this.endInclusive = toEnd ? true : endInclusive;
        }

        //----- Basic Map implementation that defers to backing when possible

        @Override
        public boolean isEmpty() {
            return (fromStart && toEnd) ? backingMap.size == 0 : entrySet().isEmpty();
        }

        @Override
        public int size() {
            return (fromStart && toEnd) ? backingMap.size : entrySet().size();
        }

        @Override
        public boolean containsKey(Object key) {
            Number numericKey = (Number) key;
            return containsKey(numericKey.intValue());
        }

        public boolean containsKey(int key) {
            return isInRange(key) && backingMap.containsKey(key);
        }

        @Override
        public final E put(UnsignedInteger key, E value) {
            return put(key.intValue(), value);
        }

        public final E put(int key, E value) {
            if (!isInRange(key)) {
                throw new IllegalArgumentException("The given key is out of range for this ranged sub-map");
            }

            return backingMap.put(key, value);
        }

        @Override
        public final E get(Object key) {
            Number numericKey = (Number) key;
            return get(numericKey.intValue());
        }

        public final E get(int key) {
            return !isInRange(key) ? null :  backingMap.get(key);
        }

        @Override
        public final E remove(Object key) {
            Number numericKey = (Number) key;
            return remove(numericKey.intValue());
        }

        public final E remove(int key) {
            return !isInRange(key) ? null : backingMap.remove(key);
        }

        @Override
        public final Map.Entry<UnsignedInteger, E> ceilingEntry(UnsignedInteger key) {
            return SplayMap.export(getCeilingEntry(key.intValue()));
        }

        @Override
        public final UnsignedInteger ceilingKey(UnsignedInteger key) {
            SplayedEntry<E> result = getCeilingEntry(key.intValue());
            return result == null ? null : result.getKey();
        }

        @Override
        public final Map.Entry<UnsignedInteger, E> higherEntry(UnsignedInteger key) {
            return SplayMap.export(getHigherEntry(key.intValue()));
        }

        @Override
        public final UnsignedInteger higherKey(UnsignedInteger key) {
            SplayedEntry<E> result = getHigherEntry(key.intValue());
            return result == null ? null : result.getKey();
        }

        @Override
        public final Map.Entry<UnsignedInteger, E> floorEntry(UnsignedInteger key) {
            return SplayMap.export(getFloorEntry(key.intValue()));
        }

        @Override
        public final UnsignedInteger floorKey(UnsignedInteger key) {
            SplayedEntry<E> result = getFloorEntry(key.intValue());
            return result == null ? null : result.getKey();
        }

        @Override
        public final Map.Entry<UnsignedInteger, E> lowerEntry(UnsignedInteger key) {
            return SplayMap.export(getLowerEntry(key.intValue()));
        }

        @Override
        public final UnsignedInteger lowerKey(UnsignedInteger key) {
            SplayedEntry<E> result = getLowerEntry(key.intValue());
            return result == null ? null : result.getKey();
        }

        @Override
        public final UnsignedInteger firstKey() {
            SplayedEntry<E> result = getLowestEntry();
            if (result != null) {
                return result.getKey();
            }

            throw new NoSuchElementException();
        }

        @Override
        public final UnsignedInteger lastKey() {
            SplayedEntry<E> result = getHighestEntry();
            if (result != null) {
                return result.getKey();
            }

            throw new NoSuchElementException();
        }

        @Override
        public final Map.Entry<UnsignedInteger, E> firstEntry() {
            return SplayMap.export(getLowestEntry());
        }

        @Override
        public final Map.Entry<UnsignedInteger, E> lastEntry() {
            return SplayMap.export(getHighestEntry());
        }

        @Override
        public final Map.Entry<UnsignedInteger, E> pollFirstEntry() {
            SplayedEntry<E> result = getLowestEntry();
            Map.Entry<UnsignedInteger, E> exported = SplayMap.export(result);
            if (exported != null) {
                backingMap.delete(result);
            }

            return exported;
        }

        @Override
        public final Map.Entry<UnsignedInteger, E> pollLastEntry() {
            SplayedEntry<E> result = getHighestEntry();
            Map.Entry<UnsignedInteger, E> exported = SplayMap.export(result);
            if (exported != null) {
                backingMap.delete(result);
            }

            return exported;
        }

        @Override
        public SortedMap<UnsignedInteger, E> subMap(UnsignedInteger fromKey, UnsignedInteger toKey) {
            return subMap(fromKey, true, toKey, false);
        }

        @Override
        public SortedMap<UnsignedInteger, E> headMap(UnsignedInteger toKey) {
            return headMap(toKey, false);
        }

        @Override
        public SortedMap<UnsignedInteger, E> tailMap(UnsignedInteger fromKey) {
            return tailMap(fromKey, true);
        }

        @Override
        public final Set<UnsignedInteger> keySet() {
            return navigableKeySet();
        }

        @Override
        public NavigableSet<UnsignedInteger> descendingKeySet() {
            return descendingMap().navigableKeySet();
        }

        @Override
        public final NavigableSet<UnsignedInteger> navigableKeySet() {
            return (navigableSubMapKeySet != null) ?
                navigableSubMapKeySet : (navigableSubMapKeySet = new NavigableSubMapKeySet(this));
        }

        //----- The abstract API that sub-classes will define

        /**
         * Returns an iterator appropriate to the sub map implementation
         * which may be ascending or descending but must be the inverse of
         * the direction of iterator returned from the {@link #descendingKeyIterator()}
         * method.
         *
         * @return an iterator that operates over the keys in this sub map range.
         */
        abstract Iterator<UnsignedInteger> keyIterator();

        /**
         * Returns an iterator appropriate to the sub map implementation
         * which may be ascending or descending but must be the inverse of
         * the direction of iterator returned from the {@link #keyIterator()}
         * method.
         *
         * @return an iterator that operates over the keys in this sub map range.
         */
        abstract Iterator<UnsignedInteger> descendingKeyIterator();

        //----- Sub Map collection types

        /**
         * Specialized iterator for the sub-map type that iterators on a generic type
         * but internally contains splayed entries from the splay map tree.
         *
         * @param <T>
         */
        protected abstract class NavigableSubMapIterator<T> implements Iterator<T> {
            SplayedEntry<E> lastReturned;
            SplayedEntry<E> next;
            final int limitKey;
            int expectedModCount;

            NavigableSubMapIterator(SplayedEntry<E> start, SplayedEntry<E> limit) {
                expectedModCount = backingMap.modCount;
                lastReturned = null;
                next = start;
                limitKey = limit != null ? limit.key : UnsignedInteger.MAX_VALUE.intValue();
            }

            @Override
            public abstract boolean hasNext();

            @Override
            public abstract T next();

            @Override
            public final void remove() {
                if (lastReturned == null) {
                    throw new IllegalStateException();
                }
                if (backingMap.modCount != expectedModCount) {
                    throw new ConcurrentModificationException();
                }

                backingMap.delete(lastReturned);
                lastReturned = null;
                expectedModCount = backingMap.modCount;
            }

            final boolean hasNextEntry() {
                return next != null && SplayMap.compare(next.key, limitKey) <= 0;
            }

            final SplayedEntry<E> nextEntry() {
                SplayedEntry<E> e = next;

                if (e == null || SplayMap.compare(next.key, limitKey) > 0) {
                    throw new NoSuchElementException();
                }
                if (backingMap.modCount != expectedModCount) {
                    throw new ConcurrentModificationException();
                }

                next = successor(e);
                lastReturned = e;
                return e;
            }

            final boolean hasPrevEntry() {
                return next != null && SplayMap.compare(next.key, limitKey) >= 0;
            }

            final SplayedEntry<E> previousEntry() {
                SplayedEntry<E> e = next;

                if (e == null || SplayMap.compare(next.key, limitKey) < 0) {
                    throw new NoSuchElementException();
                }
                if (backingMap.modCount != expectedModCount) {
                    throw new ConcurrentModificationException();
                }

                next = predecessor(e);
                lastReturned = e;
                return e;
            }
        }

        final class NavigableSubMapEntryIterator extends NavigableSubMapIterator<Map.Entry<UnsignedInteger, E>> {
            NavigableSubMapEntryIterator(SplayedEntry<E> first, SplayedEntry<E> fence) {
                super(first, fence);
            }

            @Override
            public boolean hasNext() {
                return hasNextEntry();
            }

            @Override
            public Map.Entry<UnsignedInteger, E> next() {
                return nextEntry();
            }
        }

        final class DescendingNavigableSubMapEntryIterator extends NavigableSubMapIterator<Map.Entry<UnsignedInteger, E>> {
            DescendingNavigableSubMapEntryIterator(SplayedEntry<E> first, SplayedEntry<E> fence) {
                super(first, fence);
            }

            @Override
            public boolean hasNext() {
                return hasPrevEntry();
            }

            @Override
            public Map.Entry<UnsignedInteger, E> next() {
                return previousEntry();
            }
        }

        final class NavigableSubMapKeyIterator extends NavigableSubMapIterator<UnsignedInteger> {
            NavigableSubMapKeyIterator(SplayedEntry<E> first, SplayedEntry<E> fence) {
                super(first, fence);
            }

            @Override
            public boolean hasNext() {
                return hasNextEntry();
            }

            @Override
            public UnsignedInteger next() {
                return nextEntry().getKey();
            }
        }

        final class DescendingNavigableSubMapKeyIterator extends NavigableSubMapIterator<UnsignedInteger> {
            DescendingNavigableSubMapKeyIterator(SplayedEntry<E> first, SplayedEntry<E> fence) {
                super(first, fence);
            }

            @Override
            public boolean hasNext() {
                return hasPrevEntry();
            }

            @Override
            public UnsignedInteger next() {
                return previousEntry().getKey();
            }
        }

        protected abstract class NavigableSubMapEntrySet extends AbstractSet<Map.Entry<UnsignedInteger, E>> {
            private transient int size = -1;
            private transient int sizeModCount;

            @Override
            public int size() {
                if ((!fromStart || !toEnd) && (size == -1 || sizeModCount != backingMap.modCount)) {
                    sizeModCount = backingMap.modCount;
                    size = 0;
                    Iterator<?> i = iterator();
                    while (i.hasNext()) {
                        size++;
                        i.next();
                    }

                    return size;
                }

                return backingMap.size;
            }

            @Override
            public boolean isEmpty() {
                SplayedEntry<E> n = getLowestEntry();
                return n == null || isToHigh(n.key);
            }

            @Override
            public boolean contains(Object o) {
                if (o instanceof Map.Entry) {
                    Map.Entry<?,?> entry = (Map.Entry<?,?>) o;
                    Number key = Number.class.cast(entry.getKey());

                    if (!isInRange(key.intValue())) {
                        return false;
                    }

                    SplayedEntry<E> node = backingMap.findEntry(key.intValue());

                    if (node != null) {
                        return Objects.equals(node.getValue(), entry.getValue());
                    }
                }

                return false;
            }

            @Override
            public boolean remove(Object o) {
                if (o instanceof Map.Entry) {
                    Map.Entry<?,?> entry = (Map.Entry<?,?>) o;
                    Number key = Number.class.cast(entry.getKey());

                    if (!isInRange(key.intValue())) {
                        return false;
                    }

                    SplayedEntry<E> node = backingMap.findEntry(key.intValue());

                    if (node != null) {
                        backingMap.delete(node);
                        return Objects.equals(node.getValue(), entry.getValue());
                    }
                }

                return false;
            }
        }

        //----- Abstract access API which will be inverted based on sub map type

        abstract SplayedEntry<E> getLowestEntry();

        abstract SplayedEntry<E> getHighestEntry();

        abstract SplayedEntry<E> getCeilingEntry(int key);

        abstract SplayedEntry<E> getHigherEntry(int key);

        abstract SplayedEntry<E> getFloorEntry(int key);

        abstract SplayedEntry<E> getLowerEntry(int key);

        //----- Internal API that aids this and other sub-classes

        protected final SplayedEntry<E> lowestPossibleEntry() {
            SplayedEntry<E> e =
                startInclusive ? backingMap.getCeilingEntry(startKey) : backingMap.getHigherEntry(startKey);

            return (e == null || isToHigh(e.key)) ? null : e;
        }

        protected final SplayedEntry<E> highestPossibleEntry() {
            SplayedEntry<E> e =
                endInclusive ? backingMap.getFloorEntry(endKey) : backingMap.getLowerEntry(endKey);

            return (e == null || isToLow(e.key)) ? null : e;
        }

        protected final SplayedEntry<E> entryOrSuccessor(int key) {
            if (isToLow(key)) {
                return lowestPossibleEntry();
            }
            SplayedEntry<E> e = backingMap.getCeilingEntry(key);

            return (e == null || isToHigh(e.key)) ? null : e;
        }

        protected final SplayedEntry<E> entrySuccessor(int key) {
            if (isToLow(key)) {
                return lowestPossibleEntry();
            }
            SplayedEntry<E> e = backingMap.getHigherEntry(key);

            return (e == null || isToHigh(e.key)) ? null : e;
        }

        protected final SplayedEntry<E> entryOrPredecessor(int key) {
            if (isToHigh(key)) {
                return highestPossibleEntry();
            }
            SplayedEntry<E> e = backingMap.getFloorEntry(key);

            return (e == null || isToLow(e.key)) ? null : e;
        }

        protected final SplayedEntry<E> entryPredecessor(int key) {
            if (isToHigh(key)) {
                return highestPossibleEntry();
            }
            SplayedEntry<E> e = backingMap.getLowerEntry(key);

            return (e == null || isToLow(e.key)) ? null : e;
        }

        protected final void checkInRange(int fromKey, boolean fromInclusive, int toKey, boolean toInclusive) {
            if (!isInRange(fromKey, fromInclusive)) {
                throw new IllegalArgumentException("Given from key is out of range of this sub map view: " + fromKey);
            }
            if (!isInRange(toKey, toInclusive)) {
                throw new IllegalArgumentException("Given to key is out of range of this sub map view: " + toKey);
            }
        }

        protected final boolean isInRange(int key) {
            return !isToLow(key) && !isToHigh(key);
        }

        final boolean isInCapturedRange(int key) {
            return SplayMap.compare(key, startKey) >= 0 && SplayMap.compare(endKey, key) >= 0;
        }

        final boolean isInRange(int key, boolean inclusive) {
            return inclusive ? isInRange(key) : isInCapturedRange(key);
        }

        protected final boolean isToLow(int key) {
            int result = SplayMap.compare(key, startKey);
            if (result < 0 || result == 0 && !startInclusive) {
                return true;
            } else {
                return false;
            }
        }

        protected final boolean isToHigh(int key) {
            int result = SplayMap.compare(key, endKey);
            if (result > 0 || result == 0 && !endInclusive) {
                return true;
            } else {
                return false;
            }
        }
    }

    protected static final class AscendingSubMap<V> extends NavigableSubMap<V> {

        AscendingSubMap(SplayMap<V> m,
                        boolean fromStart, int fromKey, boolean startInclusive,
                        boolean toEnd, int endKey, boolean endInclusive) {
            super(m, fromStart, fromKey, startInclusive, toEnd, endKey, endInclusive);
        }

        private transient NavigableMap<UnsignedInteger, V> descendingMapView;
        private transient NavigableSubMapEntrySet navigableEntrySet;

        @Override
        public Comparator<? super UnsignedInteger> comparator() {
            return COMPARATOR;
        }

        @Override
        public NavigableMap<UnsignedInteger, V> descendingMap() {
            return descendingMapView != null ? descendingMapView :
                (descendingMapView = new DescendingSubMap<>(backingMap,
                    fromStart, startKey, startInclusive, toEnd, endKey, endInclusive));
        }

        @Override
        public NavigableMap<UnsignedInteger, V> subMap(UnsignedInteger fromKey, boolean fromInclusive,
                                                       UnsignedInteger toKey, boolean toInclusive) {
            checkInRange(fromKey.intValue(), fromInclusive, toKey.compareTo(0), toInclusive);
            return new AscendingSubMap<>(backingMap,
                false, fromKey.intValue(), fromInclusive, false, toKey.intValue(), toInclusive);
        }

        @Override
        public NavigableMap<UnsignedInteger, V> headMap(UnsignedInteger toKey, boolean inclusive) {
            isInRange(toKey.intValue(), inclusive);
            return new AscendingSubMap<>(backingMap,
                fromStart, startKey, startInclusive, false, toKey.intValue(), inclusive);
        }

        @Override
        public NavigableMap<UnsignedInteger, V> tailMap(UnsignedInteger fromKey, boolean inclusive) {
            isInRange(fromKey.intValue(), inclusive);
            return new AscendingSubMap<>(backingMap,
                false, fromKey.intValue(), inclusive, toEnd, endKey, endInclusive);
        }

        @Override
        public Set<Entry<UnsignedInteger, V>> entrySet() {
            return navigableEntrySet != null ?
                navigableEntrySet : (navigableEntrySet = new AscendingNavigableSubMapEntrySet());
        }

        @Override
        Iterator<UnsignedInteger> keyIterator() {
            return new NavigableSubMapKeyIterator(getLowestEntry(), getHighestEntry());
        }

        @Override
        Iterator<UnsignedInteger> descendingKeyIterator() {
            return new DescendingNavigableSubMapKeyIterator(getLowestEntry(), getHighestEntry());
        }

        @Override
        SplayedEntry<V> getLowestEntry() {
            return super.lowestPossibleEntry();
        }

        @Override
        SplayedEntry<V> getHighestEntry() {
            return super.highestPossibleEntry();
        }

        @Override
        SplayedEntry<V> getCeilingEntry(int key) {
            return super.entryOrSuccessor(key);
        }

        @Override
        SplayedEntry<V> getHigherEntry(int key) {
            return super.entrySuccessor(key);
        }

        @Override
        SplayedEntry<V> getFloorEntry(int key) {
            return super.entryOrPredecessor(key);
        }

        @Override
        SplayedEntry<V> getLowerEntry(int key) {
            return super.entryPredecessor(key);
        }

        private final class AscendingNavigableSubMapEntrySet extends NavigableSubMapEntrySet {

            @Override
            public Iterator<Entry<UnsignedInteger, V>> iterator() {
                return new NavigableSubMapEntryIterator(lowestPossibleEntry(), highestPossibleEntry());
            }
        }
    }

    protected static final class DescendingSubMap<V> extends NavigableSubMap<V> {

        DescendingSubMap(SplayMap<V> m,
                        boolean fromStart, int fromKey, boolean startInclusive,
                        boolean toEnd, int endKey, boolean endInclusive) {
            super(m, fromStart, fromKey, startInclusive, toEnd, endKey, endInclusive);
        }

        private transient NavigableMap<UnsignedInteger, V> ascendingMapView;
        private transient NavigableSubMapEntrySet navigableEntrySet;

        @Override
        public Comparator<? super UnsignedInteger> comparator() {
            return REVERSE_COMPARATOR;
        }

        @Override
        public NavigableMap<UnsignedInteger, V> descendingMap() {
            return ascendingMapView != null ? ascendingMapView :
                (ascendingMapView = new AscendingSubMap<>(backingMap,
                    fromStart, startKey, startInclusive, toEnd, endKey, endInclusive));
        }

        @Override
        public NavigableMap<UnsignedInteger, V> subMap(UnsignedInteger fromKey, boolean fromInclusive,
                                                       UnsignedInteger toKey, boolean toInclusive) {
            checkInRange(fromKey.intValue(), fromInclusive, toKey.intValue(), toInclusive);
            return new DescendingSubMap<>(backingMap,
                false, toKey.intValue(), toInclusive, false, fromKey.intValue(), fromInclusive);
        }

        @Override
        public NavigableMap<UnsignedInteger, V> headMap(UnsignedInteger toKey, boolean inclusive) {
            isInRange(toKey.intValue(), inclusive);
            return new DescendingSubMap<>(backingMap,
                false, toKey.intValue(), inclusive, toEnd, endKey, endInclusive);
        }

        @Override
        public NavigableMap<UnsignedInteger, V> tailMap(UnsignedInteger fromKey, boolean inclusive) {
            isInRange(fromKey.intValue(), inclusive);
            return new DescendingSubMap<>(backingMap,
                fromStart, startKey, startInclusive, false, fromKey.intValue(), inclusive);
        }

        @Override
        public Set<Entry<UnsignedInteger, V>> entrySet() {
            return navigableEntrySet != null ?
                navigableEntrySet : (navigableEntrySet = new DescendingNavigableSubMapEntrySet());
        }

        @Override
        Iterator<UnsignedInteger> keyIterator() {
            return new DescendingNavigableSubMapKeyIterator(getHighestEntry(), getLowestEntry());
        }

        @Override
        Iterator<UnsignedInteger> descendingKeyIterator() {
            return new NavigableSubMapKeyIterator(getHighestEntry(), getLowestEntry());
        }

        @Override
        SplayedEntry<V> getLowestEntry() {
            return super.highestPossibleEntry();
        }

        @Override
        SplayedEntry<V> getHighestEntry() {
            return super.lowestPossibleEntry();
        }

        @Override
        SplayedEntry<V> getCeilingEntry(int key) {
            return super.entryPredecessor(key);
        }

        @Override
        SplayedEntry<V> getHigherEntry(int key) {
            return super.entryPredecessor(key);
        }

        @Override
        SplayedEntry<V> getFloorEntry(int key) {
            return super.entryOrSuccessor(key);
        }

        @Override
        SplayedEntry<V> getLowerEntry(int key) {
            return super.entryOrSuccessor(key);
        }

        @Override
        public void forEach(BiConsumer<? super UnsignedInteger, ? super V> action) {
            Objects.requireNonNull(action);
            for (Map.Entry<UnsignedInteger, V> entry : entrySet()) {
                UnsignedInteger k;
                V v;
                try {
                    k = entry.getKey();
                    v = entry.getValue();
                } catch (IllegalStateException ise) {
                    // this usually means the entry is no longer in the map.
                    throw new ConcurrentModificationException(ise);
                }
                action.accept(k, v);
            }
        }

        private final class DescendingNavigableSubMapEntrySet extends NavigableSubMapEntrySet {

            @Override
            public Iterator<Entry<UnsignedInteger, V>> iterator() {
                return new DescendingNavigableSubMapEntryIterator(highestPossibleEntry(), lowestPossibleEntry());
            }
        }
    }
}
