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

package org.apache.qpid.protonj2.resource;

/**
 * A referenced resource is one in which a number of objects can claim reference
 * and the resource will not close or free claimed resources until all references
 * have been released.
 *
 * @param <T> The resource type that whose references are being tracked.
 */
public abstract class SharedResource<T extends Resource<T>> implements Resource<T>, AutoCloseable {

    private static final int CLOSED = -1;
    private static final int TRANSFERRED = -2;

    private int count;

    protected final T acquire() {
        if (count < 0) {
            throw resourceIsClosedException();
        }

        count++; // Now shared and must have multiple closes

        return self();
    }

    @Override
    public final void close() {
        int current = count;

        if (current-- == 0) {
            count = CLOSED;
            releaseResourceOwnership();
        } else if (current >= 0) {
            --count;
        }
    }

    @Override
    public final T transfer() {
        if (count < 0) {
            throw resourceIsClosedException();
        }

        try {
            return transferTheResource();
        } finally {
            count = TRANSFERRED;
            releaseResourceOwnership();
        }
    }

    @Override
    public final boolean isClosed() {
        return count < 0;
    }

    protected final boolean isShared() {
        return count > 0;
    }

    protected abstract void releaseResourceOwnership();

    protected abstract T transferTheResource();

    protected abstract RuntimeException resourceIsClosedException();

    @SuppressWarnings("unchecked")
    private T self() {
        return (T) this;
    }
}
