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
package org.messaginghub.amqperative.transport;

import java.io.IOException;
import java.security.Principal;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.messaginghub.amqperative.SslOptions;
import org.messaginghub.amqperative.TransportOptions;

/**
 * Base class for all QpidJMS Transport instances.
 */
public interface Transport {

    /**
     * Performs the connect operation for the implemented Transport type
     * such as a TCP socket connection, SSL/TLS handshake etc.
     *
     * @param initRoutine
     * 			a runnable initialization method that is executed in the context
     *          of the transport's IO thread to allow thread safe setup of resources
     *          that will be run from the transport executor service.
     *
     * @return A ScheduledThreadPoolExecutor that can run work on the Transport IO thread.
     *
     * @throws IOException if an error occurs while attempting the connect.
     */
    ScheduledExecutorService connect(Runnable initRoutine) throws IOException;

    /**
     * @return true if transport is connected or false if the connection is down.
     */
    boolean isConnected();

    /**
     * @return true if transport is connected using a secured channel (SSL).
     */
    boolean isSecure();

    /**
     * Close the Transport, no additional send operations are accepted.
     *
     * @throws IOException if an error occurs while closing the connection.
     */
    void close() throws IOException;

    /**
     * Gets a buffer allocator that can produce {@link ProtonBuffer} instance that may be
     * optimized for use with the underlying transport implementation.
     *
     * @return a {@link ProtonBufferAllocator} that creates transport friendly buffers.
     */
    ProtonBufferAllocator getBufferAllocator();

    /**
     * Writes a chunk of data over the Transport connection without performing an
     * explicit flush on the transport.
     *
     * @param output
     *        The buffer of data that is to be transmitted.
     *
     * @return this Transport instance.
     *
     * @throws IOException if an error occurs during the write operation.
     */
    Transport write(ProtonBuffer output) throws IOException;

    /**
     * Writes a chunk of data over the Transport connection and requests a flush of
     * all pending queued write operations
     *
     * @param output
     *        The buffer of data that is to be transmitted.
     *
     * @return this Transport instance.
     *
     * @throws IOException if an error occurs during the write operation.
     */
    Transport writeAndFlush(ProtonBuffer output) throws IOException;

    /**
     * Request a flush of all pending writes to the underlying connection.
     *
     * @return this Transport instance.
     *
     * @throws IOException if an error occurs during the flush operation.
     */
    Transport flush() throws IOException;

    /**
     * Gets the currently set TransportListener instance
     *
     * @return the current TransportListener or null if none set.
     */
    TransportListener getTransportListener();

    /**
     * @return the {@link ThreadFactory} used to create the IO thread for this Transport
     */
    ThreadFactory getThreadFactory();

    /**
     * @return a {@link TransportOptions} instance copied from the immutable options given at create time..
     */
    TransportOptions getTransportOptions();

    /**
     * @return a {@link SslOptions} instance copied from the immutable options given at create time..
     */
    SslOptions getSslOptions();

    /**
     * @return the host name or IP address that the transport connects to.
     */
    String getHost();

    /**
     * @return the port that the transport connects to.
     */
    int getPort();

    /**
     * @return the local principal for a Transport that is using a secure connection.
     */
    Principal getLocalPrincipal();

}
