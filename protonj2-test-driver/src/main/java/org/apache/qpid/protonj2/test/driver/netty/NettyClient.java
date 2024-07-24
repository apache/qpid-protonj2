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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;

/**
 * Base API for Netty Client implementations
 */
public interface NettyClient extends AutoCloseable {

    /**
     * Connect to the specified host on the given port.
     *
     * @param host
     * 		The host to connect to
     * @param port
     * 		The port on the host to connect to.
     *
     * @throws IOException if the connect fails immediately.
     */
    void connect(String host, int port) throws IOException;

    /**
     * @return a wrapped event loop that exposes common netty event loop APIs
     */
    NettyEventLoop eventLoop();

    /**
     * Writes the given {@link ByteBuffer} to the client IO layer.
     *
     * @param buffer
     * 		The buffer of bytes to write.
     */
    void write(ByteBuffer buffer);

    /**
     * @return is the client currently connected.
     */
    boolean isConnected();

    /**
     * @return is the connection secure.
     */
    boolean isSecure();

    /**
     * @return a URI that represents the client connection.
     */
    URI getRemoteURI();

    /**
     * @return true if the connected client has WS compression activated by the server.
     */
    boolean isWSCompressionActive();

}
