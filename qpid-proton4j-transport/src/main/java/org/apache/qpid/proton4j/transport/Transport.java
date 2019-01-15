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
package org.apache.qpid.proton4j.transport;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonBufferAllocator;

/**
 * AMQP Transport interface.
 *
 * TODO Possible we just make this the "Engine" and merge into engine module as one things
 *      that could be created and the engine to create events for client and server.
 */
public interface Transport {

    /**
     * Feeds incoming bytes to the transport for processing.
     *
     * @param buffer
     *      The buffer containing incoming bytes.
     *
     * @throws IOException if an error occurs processing the data.
     */
    void processIncoming(ProtonBuffer buffer) throws IOException;

    /**
     * Write the given AMQP Header into the transport.
     *
     * @param header
     *      The header to write through this Transport.
     *
     * @throws IOException if an error occurs processing the data.
     */
    void write(AMQPHeader header) throws IOException;

    /**
     * Write the given AMQP performative into the transport.
     *
     * @param performative
     *      The AMQP performative to write via this Transport
     * @param channel
     *      The channel that the perfromative is assigned to.
     * @param payload
     *      The binary payload to transmit with this performative
     * @param payloadToLarge
     *      runnable callback to inform when the frame generated will truncate the body.
     *
     * @throws IOException if an error occurs processing the data.
     */
    void write(Performative performative, short channel, ProtonBuffer body, Runnable payloadToLarge) throws IOException;

    /**
     * Write the given SASL performative into the transport.
     *
     * @param performative
     *      The SASL performative to write via this Transport
     *
     * @throws IOException if an error occurs processing the data.
     */
    void write(SaslPerformative performative) throws IOException;

    /**
     * Sets the ProtonBufferAllocator used by this Transport.
     * <p>
     * When copying data, encoding types or otherwise needing to allocate memory
     * storage the Transport will use the assigned {@link ProtonBufferAllocator}.
     * If no allocator is assigned the Transport will use the default allocator.
     *
     * @param allocator
     *      The Allocator instance to use from this Transport.
     */
    void setBufferAllocator(ProtonBufferAllocator allocator);

    /**
     * @return the currently assigned {@link ProtonBufferAllocator}.
     */
    ProtonBufferAllocator getBufferAllocator();

    /**
     * Sets a TransportListener to be notified of events on this Transport
     *
     * @param listener
     *      The TransportListener to notify
     */
    void setTransportListener(TransportListener listener);

    /**
     * Gets the currently configured TransportListener instance.
     *
     * @return the currently configured TransportListener.
     */
    TransportListener getTransportListener();

    /**
     * Gets the TransportPipeline for this Transport.
     *
     * @return the {@link TransportPipeline} for this {@link Transport}.
     */
    TransportPipeline getPipeline();

}
