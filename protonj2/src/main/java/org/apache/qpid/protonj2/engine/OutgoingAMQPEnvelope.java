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

import java.util.function.Consumer;

import org.apache.qpid.protonj2.types.transport.Performative;
import org.apache.qpid.protonj2.types.transport.Performative.PerformativeHandler;

/**
 * Frame object that carries an AMQP Performative
 */
public class OutgoingAMQPEnvelope extends PerformativeEnvelope<Performative> {

    /**
     * The frame type value to used when encoding the outgoing AMQP frame.
     */
    public static final byte AMQP_FRAME_TYPE = (byte) 0;

    private AMQPPerformativeEnvelopePool<OutgoingAMQPEnvelope> pool;

    private Consumer<Performative> payloadToLargeHandler = OutgoingAMQPEnvelope::defaultPayloadToLargeHandler;
    private Runnable frameWriteCompleteHandler;

    OutgoingAMQPEnvelope() {
        this(null);
    }

    OutgoingAMQPEnvelope(AMQPPerformativeEnvelopePool<OutgoingAMQPEnvelope> pool) {
        super(AMQP_FRAME_TYPE);

        this.pool = pool;
    }

    /**
     * Configures a handler to be invoked if the payload that is being transmitted with this
     * performative is to large to allow encoding the frame within the maximum configured AMQP
     * frame size limit.
     *
     * @param payloadToLargeHandler
     *      Handler that will update the Performative to reflect that more than one frame is required.
     *
     * @return this {@link OutgoingAMQPEnvelope} instance.
     */
    public OutgoingAMQPEnvelope setPayloadToLargeHandler(Consumer<Performative> payloadToLargeHandler) {
        if (payloadToLargeHandler != null) {
            this.payloadToLargeHandler = payloadToLargeHandler;
        } else {
            this.payloadToLargeHandler = OutgoingAMQPEnvelope::defaultPayloadToLargeHandler;
        }

        return this;
    }

    /**
     * Called when the encoder determines that the encoding of the {@link Performative} plus any
     * payload value is to large for a single AMQP frame.  The configured handler should update
     * the {@link Performative} in preparation for encoding as a split framed AMQP transfer.
     *
     * @return this {@link OutgoingAMQPEnvelope} instance
     */
    public OutgoingAMQPEnvelope handlePayloadToLarge() {
        payloadToLargeHandler.accept(getBody());
        return this;
    }

    /**
     * Configures a handler to be invoked when a write operation that was handed off to the I/O layer
     * has completed indicated that a single frame portion of the payload has been fully written.
     *
     * @param frameWriteCompleteHandler
     *      Runnable handler that will update state or otherwise respond to the write of a frame.
     *
     * @return this {@link OutgoingAMQPEnvelope} instance.
     */
    public OutgoingAMQPEnvelope setFrameWriteCompletionHandler(Runnable frameWriteCompleteHandler) {
        this.frameWriteCompleteHandler = frameWriteCompleteHandler;
        return this;
    }

    /**
     * Called by the encoder when the write of a frame that comprises the transfer of the AMQP {@link Performative}
     * plus any assigned payload has completed.  If the transfer comprises multiple frame writes this handler should
     * be invoked as each frame is successfully written by the IO layer.
     *
     * @return this {@link OutgoingAMQPEnvelope} instance.
     */
    public OutgoingAMQPEnvelope handleOutgoingFrameWriteComplete() {
        if (frameWriteCompleteHandler != null) {
            frameWriteCompleteHandler.run();
        }

        release();

        return this;
    }

    /**
     * Used to release a Frame that was taken from a Frame pool in order
     * to make it available for the next input operations.  Once called the
     * contents of the Frame are invalid and cannot be used again inside the
     * same context.
     */
    public final void release() {
        initialize(null, -1, null);

        payloadToLargeHandler = OutgoingAMQPEnvelope::defaultPayloadToLargeHandler;
        frameWriteCompleteHandler = null;

        if (pool != null) {
            pool.release(this);
        }
    }

    /**
     * Invoke the correct PerformativeHandler event based on the body of this {@link OutgoingAMQPEnvelope}
     *
     * @param <E>
     * 		The type that the {@link Performative} handler expects for the context value.
     * @param handler
     * 		The handler that should be used to process the current body value.
     * @param context
     * 		The context that should be passed along for the current event.
     */
    public <E> void invoke(PerformativeHandler<E> handler, E context) {
        getBody().invoke(handler, getPayload(), getChannel(), context);
    }

    private static void defaultPayloadToLargeHandler(Performative performative) {
        throw new IllegalArgumentException(String.format(
            "Cannot transmit performative %s with payload larger than max frame size limit", performative));
    }
}
