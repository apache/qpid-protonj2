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
    private int modCount = 0;

    private Object[] backingArray;

    public RingQueue(int queueSize) {
        this.backingArray = new Object[queueSize];
    }

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
            modCount++;
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
            modCount++;
        }

        return result;
    }

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
        modCount = 0;

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
        return (value + 1) % limit;
    }

    //----- Ring Queue Iterator Implementation

    private class RingIterator implements Iterator<E> {

        private int expectedModCount = modCount;

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
            if (expectedModCount != RingQueue.this.modCount) {
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
