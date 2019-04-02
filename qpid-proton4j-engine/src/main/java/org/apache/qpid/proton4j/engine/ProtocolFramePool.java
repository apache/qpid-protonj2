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
package org.apache.qpid.proton4j.engine;

import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * Pooled of ProtocolFrame instances used to reduce allocations on incoming frames.
 */
public class ProtocolFramePool {

    public static final ProtocolFramePool DEFAULT = new ProtocolFramePool();

    public static final int DEFAULT_MAX_POOL_SIZE = 10;

    private int maxPoolSize = DEFAULT_MAX_POOL_SIZE;

    private Deque<ProtocolFrame> pool;

    public ProtocolFramePool() {
        this(ProtocolFramePool.DEFAULT_MAX_POOL_SIZE);
    }

    public ProtocolFramePool(int maxPoolSize) {
        this.pool = new ArrayDeque<>(getMaxPoolSize());
    }

    public final int getMaxPoolSize() {
        return maxPoolSize;
    }

    public ProtocolFrame take(Performative body, int channel, int frameSize, ProtonBuffer payload) {
        ProtocolFrame element = pool.poll();

        if (element == null) {
            element = createNewFrame(this);
        }

        element.initialize(body, channel, frameSize, payload);

        return element;
    }

    void release(ProtocolFrame pooledFrame) {
        if (pool.size() < getMaxPoolSize()) {
            pool.addLast(pooledFrame);
        }
    }

    protected ProtocolFrame createNewFrame(ProtocolFramePool pool) {
        return new ProtocolFrame(pool);
    }
}
