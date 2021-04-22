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
 * Pool of {@link PerformativeEnvelope} instances used to reduce allocations on incoming performatives.
 *
 * @param <E> The type of Protocol Performative to pool incoming or outgoing.
 */
public class AMQPPerformativeEnvelopePool<E extends PerformativeEnvelope<Performative>> {

	/**
	 * The default maximum pool size to use if not otherwise configured.
	 */
    public static final int DEFAULT_MAX_POOL_SIZE = 10;

    private int maxPoolSize = DEFAULT_MAX_POOL_SIZE;

    private final RingQueue<E> pool;
    private final Function<AMQPPerformativeEnvelopePool<E>, E> envelopeBuilder;

    /**
     * Create a new envelope pool using the default pool size.
     *
     * @param envelopeBuilder
     * 		The builder that will provide new envelope instances when the pool is empty.
     */
    public AMQPPerformativeEnvelopePool(Function<AMQPPerformativeEnvelopePool<E>, E> envelopeBuilder) {
        this(envelopeBuilder, AMQPPerformativeEnvelopePool.DEFAULT_MAX_POOL_SIZE);
    }

    /**
     * Create a new envelope pool using the default pool size.
     *
     * @param envelopeBuilder
     * 		The builder that will provide new envelope instances when the pool is empty.
     * @param maxPoolSize
     *      The maximum number of envelopes to hold in the pool at any given time.
     */
    public AMQPPerformativeEnvelopePool(Function<AMQPPerformativeEnvelopePool<E>, E> envelopeBuilder, int maxPoolSize) {
        this.pool = new RingQueue<>(getMaxPoolSize());
        this.maxPoolSize = maxPoolSize;
        this.envelopeBuilder = envelopeBuilder;
    }

    /**
     * @return the configured maximum pool size.
     */
    public final int getMaxPoolSize() {
        return maxPoolSize;
    }

    /**
     * Requests an envelope from the pool and if non is available creates one using the given
     * builder this pool was created with.
     *
     * @param body
     * 		The body that will be stored in the envelope.
     * @param channel
     * 		The channel that is assigned to the envelope until returned to the pool.
     * @param payload
     * 		The Binary payload that is to be encoded with the given envelope body.
     *
     * @return the envelope instance that was taken from the pool or created if the pool was empty.
     */
    @SuppressWarnings("unchecked")
    public E take(Performative body, int channel, ProtonBuffer payload) {
        return (E) pool.poll(this::supplyPooledResource).initialize(body, channel, payload);
    }

    void release(E pooledEnvelope) {
        pool.offer(pooledEnvelope);
    }

    private E supplyPooledResource() {
        return envelopeBuilder.apply(this);
    }

    /**
     * @param maxPoolSize
     *      The maximum number of protocol envelopes to store in the pool.
     *
     * @return a new {@link AMQPPerformativeEnvelopePool} that pools incoming AMQP envelopes
     */
    public static AMQPPerformativeEnvelopePool<IncomingAMQPEnvelope> incomingEnvelopePool(int maxPoolSize) {
        return new AMQPPerformativeEnvelopePool<>((pool) -> new IncomingAMQPEnvelope(pool), maxPoolSize);
    }

    /**
     * @return a new {@link AMQPPerformativeEnvelopePool} that pools incoming AMQP envelopes
     */
    public static AMQPPerformativeEnvelopePool<IncomingAMQPEnvelope> incomingEnvelopePool() {
        return new AMQPPerformativeEnvelopePool<>((pool) -> new IncomingAMQPEnvelope(pool));
    }

    /**
     * @param maxPoolSize
     *      The maximum number of protocol envelopes to store in the pool.
     *
     * @return a new {@link AMQPPerformativeEnvelopePool} that pools outgoing AMQP envelopes
     */
    public static AMQPPerformativeEnvelopePool<OutgoingAMQPEnvelope> outgoingEnvelopePool(int maxPoolSize) {
        return new AMQPPerformativeEnvelopePool<>((pool) -> new OutgoingAMQPEnvelope(pool), maxPoolSize);
    }

    /**
     * @return a new {@link AMQPPerformativeEnvelopePool} that pools outgoing AMQP envelopes
     */
    public static AMQPPerformativeEnvelopePool<OutgoingAMQPEnvelope> outgoingEnvelopePool() {
        return new AMQPPerformativeEnvelopePool<>((pool) -> new OutgoingAMQPEnvelope(pool));
    }
}
