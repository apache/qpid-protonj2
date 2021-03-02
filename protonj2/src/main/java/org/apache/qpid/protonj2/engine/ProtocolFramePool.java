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
package org.apache.qpid.protonj2.engine;

import java.util.function.Function;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.util.RingQueue;
import org.apache.qpid.protonj2.types.transport.Performative;

/**
 * Pooled of ProtocolFrame instances used to reduce allocations on incoming frames.
 *
 * @param <E> The type of Protocol Frame to pool incoming or outgoing.
 */
public class ProtocolFramePool<E extends Frame<Performative>> {

    public static final int DEFAULT_MAX_POOL_SIZE = 10;

    private int maxPoolSize = DEFAULT_MAX_POOL_SIZE;

    private final RingQueue<E> pool;
    private final Function<ProtocolFramePool<E>, E> frameBuilder;

    public ProtocolFramePool(Function<ProtocolFramePool<E>, E> frameBuilder) {
        this(frameBuilder, ProtocolFramePool.DEFAULT_MAX_POOL_SIZE);
    }

    public ProtocolFramePool(Function<ProtocolFramePool<E>, E> frameBuilder, int maxPoolSize) {
        this.pool = new RingQueue<>(getMaxPoolSize());
        this.maxPoolSize = maxPoolSize;
        this.frameBuilder = frameBuilder;
    }

    public final int getMaxPoolSize() {
        return maxPoolSize;
    }

    @SuppressWarnings("unchecked")
    public E take(Performative body, int channel, ProtonBuffer payload) {
        return (E) pool.poll(this::supplyPooledResource).initialize(body, channel, payload);
    }

    void release(E pooledFrame) {
        pool.offer(pooledFrame);
    }

    private E supplyPooledResource() {
        return frameBuilder.apply(this);
    }

    /**
     * @param maxPoolSize
     *      The maximum number of protocol frames to store in the pool.
     *
     * @return a new {@link ProtocolFramePool} that pools incoming AMQP frames
     */
    public static ProtocolFramePool<IncomingProtocolFrame> incomingFramePool(int maxPoolSize) {
        return new ProtocolFramePool<>((pool) -> new IncomingProtocolFrame(pool), maxPoolSize);
    }

    /**
     * @return a new {@link ProtocolFramePool} that pools incoming AMQP frames
     */
    public static ProtocolFramePool<IncomingProtocolFrame> incomingFramePool() {
        return new ProtocolFramePool<>((pool) -> new IncomingProtocolFrame(pool));
    }

    /**
     * @param maxPoolSize
     *      The maximum number of protocol frames to store in the pool.
     *
     * @return a new {@link ProtocolFramePool} that pools outgoing AMQP frames
     */
    public static ProtocolFramePool<OutgoingProtocolFrame> outgoingFramePool(int maxPoolSize) {
        return new ProtocolFramePool<>((pool) -> new OutgoingProtocolFrame(pool), maxPoolSize);
    }

    /**
     * @return a new {@link ProtocolFramePool} that pools outgoing AMQP frames
     */
    public static ProtocolFramePool<OutgoingProtocolFrame> outgoingFramePool() {
        return new ProtocolFramePool<>((pool) -> new OutgoingProtocolFrame(pool));
    }
}
