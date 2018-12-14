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
package org.apache.qpid.proton4j.engine.impl;

import org.apache.qpid.proton4j.transport.ProtocolFrame;
import org.apache.qpid.proton4j.transport.TransportHandlerAdapter;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;

/**
 * Transport Handler that forwards the incoming Performatives to the associated Connection
 * as well as any error encountered during the Transport processing.
 */
public class ProtonPerformativeHandler extends TransportHandlerAdapter {

    private final ProtonConnection connection;

    public ProtonPerformativeHandler(ProtonConnection connection) {
        this.connection = connection;
    }

    @Override
    public void handleRead(TransportHandlerContext context, ProtocolFrame frame) {
        // TODO - Handle errors thrown here?  Some other context ?

        // Would need to handle remote open when no local connection yet exists

        try {
            frame.getBody().invoke(connection, frame.getPayload(), connection.geTransport());
        } finally {
            frame.release();
        }
    }

    @Override
    public void transportEncodingError(TransportHandlerContext context, Throwable e) {
        // TODO signal error to the connection
    }

    @Override
    public void transportDecodingError(TransportHandlerContext context, Throwable e) {
        // TODO signal error to the connection
    }

    @Override
    public void transportFailed(TransportHandlerContext context, Throwable e) {
        // TODO signal error to the connection
    }
}
