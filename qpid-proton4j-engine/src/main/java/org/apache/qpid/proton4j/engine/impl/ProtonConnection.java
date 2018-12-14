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

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Close;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.End;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.transport.Transport;

/**
 * Implements the proton4j Connection API
 */
public class ProtonConnection implements Connection, Performative.PerformativeHandler<Transport> {

    private final Transport transport;

    /**
     * Create a new unbound Connection instance.
     */
    public ProtonConnection(Transport transport) {
        this.transport = transport;
    }

    public Transport geTransport() {
        return transport;
    }

    @Override
    public void Open() {
    }

    @Override
    public void Close() {
    }

    //----- Process all incoming events

    @Override
    public void handleOpen(Open open, Binary payload, Transport context) {
    }

    @Override
    public void handleBegin(Begin begin, Binary payload, Transport context) {
    }

    @Override
    public void handleAttach(Attach attach, Binary payload, Transport context) {
    }

    @Override
    public void handleFlow(Flow flow, Binary payload, Transport context) {
    }

    @Override
    public void handleTransfer(Transfer transfer, Binary payload, Transport context) {
    }

    @Override
    public void handleDisposition(Disposition disposition, Binary payload, Transport context) {
    }

    @Override
    public void handleDetach(Detach detach, Binary payload, Transport context) {
    }

    @Override
    public void handleEnd(End end, Binary payload, Transport context) {
    }

    @Override
    public void handleClose(Close close, Binary payload, Transport context) {
    }
}
