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

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonBufferAllocator;

/**
 * AMQP Transport interface.
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
     * Write the given protocol frame into the transport.
     *
     * @param frame
     *      The protocol frame to write into the transport.
     *
     * @throws IOException if an error occurs processing the data.
     */
    void write(ProtocolFrame frame) throws IOException;

    /**
     * Flush the transport
     *
     * TODO - This implies that batching is possible, do we want that ?
     */
    void flush();

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
     * Get the maximum frame size that the transport will accept before
     * rejecting an incoming frame.
     *
     * @return the maximum frame size allowed for incoming frames.
     */
    int getMaxFrameSize();

    /**
     * Set the maximum frame size that the transport will accept before
     * rejecting an incoming frame.
     *
     * @param size
     *      the maximum frame size allowed for incoming frames.
     */
    void setMaxFrameSize(int size);

    /**
     * Gets the TransportPipeline for this Transport.
     *
     * @return the {@link TransportPipeline} for this {@link Transport}.
     */
    TransportPipeline getPipeline();

}
