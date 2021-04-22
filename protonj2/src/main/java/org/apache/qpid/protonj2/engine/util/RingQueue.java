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

import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.function.Supplier;

/**
 * Simple Ring Queue implementation that has an enforced max size value.
 *
 * @param <E> the element type that is stored in this {@link Queue} type.
 */
public class RingQueue<E> extends AbstractQueue<E> {

    private int read = 0;
    private int write = -1;
    private int size;

    private final Object[] backingArray;

    /**
     * Creates a new {@link RingQueue} instance with the given fixed Queue size.
     *
     * @param queueSize
     * 		The size to use for the ring queue.
     */
    public RingQueue(int queueSize) {
        this.backingArray = new Object[queueSize];
    }

    /**
     * Creates a new {@link RingQueue} instance with a size that matches the size of the
     * given {@link Collection} and filled with the values from that {@link Collection}.
     *
     * @param collection
     * 		the {@link Collection} whose values populates this {@link RingQueue} instance.
     */
    public RingQueue(Collection<E> collection) {
        this.backingArray = new Object[collection.size()];

        collection.forEach(value -> offer(value));
    }

    @Override
    public boolean offer(E e) {
        if (isFull()) {
            return false;
        } else {
            write = advance(write, backingArray.length);
            size++;
            backingArray[write] = e;
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public E poll() {
        final E result;

        if (isEmpty()) {
            result = null;
        } else {
            result = (E) backingArray[read];
            backingArray[read] = null;
            read = advance(read, backingArray.length);
            size--;
        }

        return result;
    }

    /**
     * Retrieves and removes the head of this ring queue, and if the queue is currently empty
     * a new instance of the queue type is provided by invoking the given {@link Supplier}.
     *
     * @param createOnEmpty
     *     a {@link Supplier} which will return default values if the {@link RingQueue} is empty.
     *
     * @return the head element of this queue or a default instance created from the provided {@link Supplier}/
     */
    public E poll(Supplier<E> createOnEmpty) {
        if (isEmpty()) {
            return createOnEmpty.get();
        } else {
            return poll();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public E peek() {
        return isEmpty() ? null : (E) backingArray[read];
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        Objects.requireNonNull(c, "Given collection to add was null");

        if (c == this) {
            throw new IllegalArgumentException("Cannot add a Queue to itself");
        }
        if (c.size() > backingArray.length - size) {
            throw new IllegalStateException("Insuficcient sapce to add all elements of the collection to the queue");
        }

        c.forEach(value -> offer(value));

        return !c.isEmpty();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("Cannot remove other than from the Queue head methods");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        Objects.requireNonNull(c);
        throw new UnsupportedOperationException("Cannot remove other than from the Queue head methods");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        Objects.requireNonNull(c);
        throw new UnsupportedOperationException("Cannot remove other than from the Queue head methods");
    }

    @Override
    public Iterator<E> iterator() {
        return new RingIterator();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public void clear() {
        read = 0;
        write = -1;
        size = 0;

        Arrays.fill(backingArray, null);
    }

    @Override
    public boolean contains(Object value) {
        int count = size;
        int position = read;

        if (value == null) {
            while (count > 0) {
                if (backingArray[position] == null) {
                    return true;
                }
                position = advance(position, backingArray.length);
                count--;
            }
        } else {
            while (count > 0) {
                if (value.equals(backingArray[position])) {
                    return true;
                }
                position = advance(position, backingArray.length);
                count--;
            }
        }

        return false;
    }

    //----- Internal implementation

    private boolean isFull() {
        return size == backingArray.length;
    }

    private static int advance(int value, int limit) {
        return (++value) % limit;
    }

    //----- Ring Queue Iterator Implementation

    private class RingIterator implements Iterator<E> {

        private int expectedSize = size;
        private int expectedReadIndex = read;

        private E nextElement;
        private int position;
        private int remaining;
        private E lastReturned;

        public RingIterator() {
            this.nextElement = peek();
            this.position = read;
            this.remaining = size;
        }

        @Override
        public boolean hasNext() {
            return nextElement != null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public E next() {
            E entry = nextElement;

            if (nextElement == null) {
                throw new NoSuchElementException();
            }
            if (expectedSize != size || expectedReadIndex != read) {
                throw new ConcurrentModificationException();
            }

            lastReturned = entry;

            if (--remaining != 0) {
                position = advance(position, backingArray.length);
                nextElement = (E) backingArray[position];
            } else {
                nextElement = null;
            }

            return lastReturned;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
