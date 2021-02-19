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
package org.apache.qpid.protonj2.client.transport;

import java.io.IOException;
import java.net.URI;
import java.security.Principal;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.client.SslOptions;
import org.apache.qpid.protonj2.client.TransportOptions;

/**
 * Base class for all QpidJMS Transport instances.
 */
public interface Transport {

    /**
     * Performs the connect operation for the implemented Transport type such as
     * a TCP socket connection, SSL/TLS handshake etc.  The connection operation
     * itself will be performed as an asynchronous operation with the success or
     * failure being communicated to the event point
     * {@link TransportListener#transportError(Throwable)}.  If the users wishes
     * to perform a block on connect outcome the {@link #awaitConnect()} method
     * will wait for and or throw an error based on the connect outcome.
     *
     * @param host
     *      The remote host that this {@link Transport} should attempt to connect to.
     * @param port
     *      The port on the remote host that this {@link Transport} should attempt to bind to.
     * @param listener
     *      The {@link TransportListener} that will handle {@link Transport} events.
     *
     * @return this {@link Transport} instance.
     *
     * @throws IOException if an error occurs while attempting the connect.
     */
    Transport connect(String host, int port, TransportListener listener) throws IOException;

    /**
     * Waits interruptibly for the {@link Transport} to connect to the remote that was
     * indicated in the {@link #connect(String, int, TransportListener)} call.
     *
     * @throws InterruptedException
     *      If the wait mechanism was interrupted while waiting for a successful connect.
     * @throws IOException
     *      If the {@link Transport} failed to connect or was closed before connected.
     */
    void awaitConnect() throws InterruptedException, IOException;

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
     * @return this {@link Transport} instance.
     *
     * @throws IOException if an error occurs during the write operation.
     */
    Transport write(ProtonBuffer output) throws IOException;

    /**
     * Writes a chunk of data over the Transport connection without performing an
     * explicit flush on the transport.  This method allows for a completion callback
     * that is signaled when the actual low level IO operation is completed which could
     * be after this method has returned.
     *
     * @param output
     *        The buffer of data that is to be transmitted.
     * @param ioComplete
     *        A {@link Runnable} that is invoked when the IO operation completes successfully.
     *
     * @return this {@link Transport} instance.
     *
     * @throws IOException if an error occurs during the write operation.
     */
    Transport write(ProtonBuffer output, Runnable ioComplete) throws IOException;

    /**
     * Writes a chunk of data over the Transport connection and requests a flush of
     * all pending queued write operations
     *
     * @param output
     *        The buffer of data that is to be transmitted.
     *
     * @return this {@link Transport} instance.
     *
     * @throws IOException if an error occurs during the write operation.
     */
    Transport writeAndFlush(ProtonBuffer output) throws IOException;

    /**
     * Writes a chunk of data over the Transport connection and requests a flush of
     * all pending queued write operations
     *
     * @param output
     *        The buffer of data that is to be transmitted.
     * @param ioComplete
     *        A {@link Runnable} that is invoked when the IO operation completes successfully.
     *
     * @return this {@link Transport} instance.
     *
     * @throws IOException if an error occurs during the write operation.
     */
    Transport writeAndFlush(ProtonBuffer output, Runnable ioComplete) throws IOException;

    /**
     * Request a flush of all pending writes to the underlying connection.
     *
     * @return this {@link Transport} instance.
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
     * Returns a URI that contains some meaningful information about the remote connection such as a
     * scheme that reflects the transport type and the remote host and port that the connection was
     * instructed to connect to.  If called before the {@link #connect(String, int, TransportListener)}
     * method this method returns <code>null</code>.
     *
     * @return a URI that reflects a meaningful view of the {@link Transport} remote connection details.
     */
    URI getRemoteURI();

    /**
     * @return the local principal for a Transport that is using a secure connection.
     */
    Principal getLocalPrincipal();

}
