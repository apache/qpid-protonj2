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
package org.apache.qpid.protonj2.client;

import org.apache.qpid.protonj2.client.exceptions.ClientIOException;

/**
 * An event object that accompanies events fired to handlers configured in the
 * {@link ConnectionOptions} which are signaled during specific {@link Connection}
 * life-cycle stages.
 */
public class DisconnectionEvent {

    private final String host;
    private final int port;
    private final ClientIOException failureCause;

    /**
     * Creates the event object with all immutable data provided.
     *
     * @param host
     *      the host that is associated with this {@link DisconnectionEvent}
     * @param port
     *      the port that is associated with this {@link DisconnectionEvent}
     * @param failureCause
     *      the failure cause that is associated with this {@link DisconnectionEvent}
     */
    public DisconnectionEvent(String host, int port, ClientIOException failureCause) {
        this.host = host;
        this.port = port;
        this.failureCause = failureCause;
    }

    /**
     * Gets the host that is associated with this event which for a successful
     * {@link Connection} event would be the currently active host and for an
     * interrupted or failed Connection this host would indicate the host where
     * the {@link Connection} had previously been established.
     *
     * @return the host that is associated with this {@link DisconnectionEvent}
     */
    public String host() {
        return host;
    }

    /**
     * Gets the port that is associated with this event which for a successful
     * {@link Connection} event would be the currently active port and for an
     * interrupted or failed Connection this port would indicate the host where
     * the {@link Connection} had previously been established.
     *
     * @return the port that is associated with this {@link DisconnectionEvent}
     */
    public int port() {
        return port;
    }

    /**
     * Gets the failure cause that is associated with this event if the event indicates
     * an error stage in the {@link Connection} life-cycle such as a connection being
     * interrupted which might later be recovered or a failure to establish or reconnect
     * a previously established {@link Connection}.
     *
     * @return the failureCause that is associated with this {@link DisconnectionEvent}
     */
    public ClientIOException failureCause() {
        return failureCause;
    }
}
