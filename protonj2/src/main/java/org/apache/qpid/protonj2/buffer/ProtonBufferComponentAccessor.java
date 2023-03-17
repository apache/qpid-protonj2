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

package org.apache.qpid.protonj2.buffer;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Provides a way of accessing the internal components of a {@link ProtonBuffer}
 * which can be used to gain access to underlying buffer internals for IO or other
 * low level buffer operations.
 * <p>
 * A component access object is not meant to have a long life-span as it can prevent
 * the quick cleanup of buffer resources. Likewise the access object must be closed
 * upon completion of the access if proper resource cleanup is to occur as the object
 * itself ensure that there is concrete referencing of the buffer preventing a JVM
 * GC of the object should the user discard the buffer before the access object has
 * been discarded.
 * <p>
 * The general usage of the component access object should be within a try-with-resource
 * block as follows although it should be noted that if using the iteration type component
 * walk an allocation of an {@link Iterable} and an {@link Iterator} will be made:
 * <pre>{@code
 *   try (ProtonBufferComponentAccessor accessor = buffer.componentAccessor()) {
 *      for (ProtonBufferComponent component : accessor.readableComponents()) {
 *         // Access logic here....
 *      }
 *   }
 * }
 * </pre>
 * <p>
 * Or an alternative that does not create an iterator for walking the list of available
 * ProtonBufferComponents would look as follows:
 * <pre>{@code
 *   try (ProtonBufferComponentAccessor accessor = buffer.componentAccessor()) {
 *      for (ProtonBufferComponent component = accessor.first(); component != null; component = accessor.next()) {
 *         // Access logic here....
 *      }
 *   }
 * }
 * </pre>
 */
public interface ProtonBufferComponentAccessor extends AutoCloseable {

    /**
     * Safe to call close in all cases the close will not throw.
     */
    @Override
    void close();

    /**
     * Returns the first component that this access object provides which resets the
     * iteration state to the beginning.
     *
     * @return the first component in the sequence of {@link ProtonBufferComponent} instance.
     */
    ProtonBufferComponent first();

    /**
     * Returns the first readable component that this access object provides which resets the
     * iteration state to the beginning.
     *
     * @return the first readable component in the sequence of {@link ProtonBufferComponent} instance.
     */
    default ProtonBufferComponent firstReadable() {
        final ProtonBufferComponent current = first();
        if (current != null && current.getReadableBytes() == 0) {
            return nextReadable();
        } else {
            return current;
        }
    }

    /**
     * Returns the first writable component that this access object provides which resets the
     * iteration state to the beginning.
     *
     * @return the first writable component in the sequence of {@link ProtonBufferComponent} instance.
     */
    default ProtonBufferComponent firstWritable() {
        final ProtonBufferComponent current = first();
        if (current != null && current.getWritableBytes() == 0) {
            return nextWritable();
        } else {
            return current;
        }
    }

    /**
     * Returns the next component that this access object provides which can be null if either
     * the first method has never been called or the access of components has reached the end
     * of the chain of buffer components that this access object is assigned to.
     *
     * @return the first component in the sequence of {@link ProtonBufferComponent} instance.
     */
    ProtonBufferComponent next();

    /**
     * @return the next readable {@link ProtonBufferComponent} in the current chain.
     */
    default ProtonBufferComponent nextReadable() {
        return nextReadableComponent(this);
    }

    /**
     * @return the next readable {@link ProtonBufferComponent} in the current chain.
     */
    default ProtonBufferComponent nextWritable() {
        return nextWritableComponent(this);
    }

    /**
     * @return an {@link Iterable} instance over all the buffer components this instance can reach
     */
    default Iterable<ProtonBufferComponent> components() {
        return new Iterable<ProtonBufferComponent>() {

            @Override
            public Iterator<ProtonBufferComponent> iterator() {
                return componentIterator();
            }
        };
    }

    /**
     * @return an {@link Iterable} instance over all the readable buffer components this instance can reach
     */
    default Iterable<ProtonBufferComponent> readableComponents() {
        return new Iterable<ProtonBufferComponent>() {

            @Override
            public Iterator<ProtonBufferComponent> iterator() {
                return readableComponentIterator();
            }
        };
    }

    /**
     * @return an {@link Iterable} instance over all the writable buffer components this instance can reach
     */
    default Iterable<ProtonBufferComponent> writableComponents() {
        return new Iterable<ProtonBufferComponent>() {

            @Override
            public Iterator<ProtonBufferComponent> iterator() {
                return writableComponentIterator();
            }
        };
    }

    /**
     * @return an {@link Iterator} that traverses all components within the {@link ProtonBuffer}
     */
    default Iterator<ProtonBufferComponent> componentIterator() {
        return new Iterator<ProtonBufferComponent>() {

            private boolean initialized;
            private ProtonBufferComponent next;

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public ProtonBufferComponent next() {
                if (next == null && initialized) {
                    throw new NoSuchElementException();
                }

                if (!initialized) {
                    next = ProtonBufferComponentAccessor.this.first();
                    initialized = true;
                } else {
                    next = ProtonBufferComponentAccessor.this.next();
                }

                if (next == null) {
                    throw new NoSuchElementException();
                }

                return next;
            }
        };
    }

    /**
     * @return an {@link Iterator} that traverses all readable components within the {@link ProtonBuffer}
     */
    default Iterator<ProtonBufferComponent> readableComponentIterator() {
        return new Iterator<ProtonBufferComponent>() {

            private boolean initialized;
            private ProtonBufferComponent next;

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public ProtonBufferComponent next() {
                if (next == null && initialized) {
                    throw new NoSuchElementException();
                }

                if (!initialized) {
                    next = ProtonBufferComponentAccessor.this.firstReadable();
                    initialized = true;
                } else {
                    next = ProtonBufferComponentAccessor.this.nextReadable();
                }

                if (next == null) {
                    throw new NoSuchElementException();
                }

                return next;
            }
        };
    }

    /**
     * @return an {@link Iterator} that traverses all writable components within the {@link ProtonBuffer}
     */
    default Iterator<ProtonBufferComponent> writableComponentIterator() {
        return new Iterator<ProtonBufferComponent>() {

            private boolean initialized;
            private ProtonBufferComponent next;

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public ProtonBufferComponent next() {
                if (next == null && initialized) {
                    throw new NoSuchElementException();
                }

                if (!initialized) {
                    next = ProtonBufferComponentAccessor.this.firstWritable();
                    initialized = true;
                } else {
                    next = ProtonBufferComponentAccessor.this.nextWritable();
                }

                if (next == null) {
                    throw new NoSuchElementException();
                }

                return next;
            }
        };
    }

    //--- Scan for target components

    private static ProtonBufferComponent nextReadableComponent(ProtonBufferComponentAccessor accessor) {
        ProtonBufferComponent component = accessor.next();
        while (component != null && component.getReadableBytes() == 0) {
            component = accessor.next();
        }
        return component;
    }

    private static ProtonBufferComponent nextWritableComponent(ProtonBufferComponentAccessor accessor) {
        ProtonBufferComponent component = accessor.next();
        while (component != null && component.getWritableBytes() == 0) {
            component = accessor.next();
        }
        return component;
    }
}
