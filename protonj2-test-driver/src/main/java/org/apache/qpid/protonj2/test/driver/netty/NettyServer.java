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

package org.apache.qpid.protonj2.test.driver.netty;

import java.net.URI;
import java.nio.ByteBuffer;

import javax.net.ssl.SSLEngine;

/**
 * Provides the API for a Netty based server instance used by the test peer.
 */
public interface NettyServer extends AutoCloseable {

    /**
     * Starts the server instance which ends the configuration phase and allows for
     * incoming connections to be accepted.
     *
     * @throws Exception if an error occurs during server start.
     */
    void start() throws Exception;

    /**
     * Stops the server, waiting on termination of all server resources before
     * the method returns.
     *
     * @throws InterruptedException
     */
    void stop() throws InterruptedException;

    /**
     * Stops the server and returns as soon as server shutdown has been
     * successfully started but does not wait for all server resources to
     * be fully shut down.
     *
     * @throws InterruptedException
     */
    void stopAsync() throws InterruptedException;

    /**
     * Close API that calls the blocking {@link #stop()} API and awaits the
     * full server shutdown. Use this in try-with-resources style invocations.
     */
    @Override
    void close() throws InterruptedException;

    /**
     * Disconnects any connected client and leaves the server in a state where a new
     * client connection is possible.
     *
     * @throws Exception
     */
    void disconnectClient() throws Exception;

    /**
     * @return true if the server is using SSL as the transport layer.
     */
    boolean isSecureServer();

    /**
     * @return true if the server has an active SSL transport connection.
     */
    boolean hasSecureConnection();

    /**
     * @return true if the server is in a state where connections will be accepted.
     */
    boolean isAcceptingConnections();

    /**
     * @return true if there is an active connection to the server.
     */
    boolean hasClientConnection();

    /**
     * @return the remote port that the client connection is connected to.
     */
    int getClientPort();

    /**
     * @return true if a connected client has negotiated WS compression.
     */
    boolean isWSCompressionActive();

    /**
     * @return has the SSL handshake for a client completed successfully.
     */
    boolean isPeerVerified();

    /**
     * @return the {@link SSLEngine} that was configured and assigned to this server.
     */
    SSLEngine getConnectionSSLEngine();

    /**
     * @return is the server exposing a WebSocket endpoint.
     */
    boolean isWebSocketServer();

    /**
     * @return the path that was configured on the WebSocket transport.
     */
    String getWebSocketPath();

    /**
     * Assign the WebSocket path that server should use, which must be done
     * prior to starting the server.
     *
     * @param webSocketPath
     * 		The web socket path to use when accepting connections.
     */
    void setWebSocketPath(String webSocketPath);

    /**
     * @return the configured max frame size for WebSocket connections.
     */
    int getMaxFrameSize();

    /**
     * Assign the max frame size value to configure on WebSocket connection which
     * must be assigned prior to starting the server.
     *
     * @param maxFrameSize
     * 		The max frame size in bytes to assign to web socket connections.
     */
    void setMaxFrameSize(int maxFrameSize);

    /**
     * Creates an connection URI for an AMQP client that provides the proper AMQP
     * scheme format (amqp:// etc) along with the host and port values plus an optional
     * query string provided by the caller.
     *
     * @param queryString
     * 		optional query string to append to the returned URI
     *
     * @return a URI that a client could use to connect to the AMQP server.
     *
     * @throws Exception if an error occurs while creating the connection URI.
     */
    URI getConnectionURI(String queryString) throws Exception;

    /**
     * Writes the given buffer to the IO layer.
     *
     * @param buffer
     * 		The buffer to send to the remote connection.
     */
    void write(ByteBuffer buffer);

    /**
     * @return the event loop that the server processing runs within.
     */
    NettyEventLoop eventLoop();

    /**
     * Gets the server port that new connections are accepted on, this should
     * only be called after the {@link #start()} method has been called..
     *
     * @return the server port where connections should be made.
     */
    int getServerPort();

}