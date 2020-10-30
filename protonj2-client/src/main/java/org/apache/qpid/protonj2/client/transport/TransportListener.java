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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;

/**
 * Listener interface that should be implemented by users of the various
 * QpidJMS Transport classes.
 */
public interface TransportListener {

    /**
     * Called immediately before the transport attempts to connect to the remote peer
     * but following all {@link Transport} initialization.  The Transport configuration
     * is now static and the event handler can update any internal state or configure
     * additional resources based on the configured and prepared {@link Transport}.
     *
     * @param transport
     *      The transport that is now fully connected and ready to perform IO operations.
     */
    void transportInitialized(Transport transport);

    /**
     * Called after the transport has successfully connected to the remote and performed any
     * required handshakes such as SSL or Web Sockets handshaking and the connection is now
     * considered open.
     *
     * @param transport
     *      The transport that is now fully connected and ready to perform IO operations.
     */
    void transportConnected(Transport transport);

    /**
     * Called when new incoming data has become available for processing by the {@link Transport}
     * user.
     *
     * @param incoming
     *        the next incoming packet of data.
     */
    void transportRead(ProtonBuffer incoming);

    /**
     * Called when an error occurs during normal Transport operations such as SSL handshake
     * or remote connection dropped.  Once this error callback is triggered the {@link Transport}
     * is considered to be failed and should be closed.
     *
     * @param cause
     *        the error that triggered this event.
     */
    void transportError(Throwable cause);

}
