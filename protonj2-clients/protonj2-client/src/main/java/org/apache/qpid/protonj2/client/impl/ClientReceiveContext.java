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
package org.apache.qpid.protonj2.client.impl;

import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.RawInputStream;
import org.apache.qpid.protonj2.client.RawInputStreamOptions;
import org.apache.qpid.protonj2.client.ReceiveContext;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Receive Context used to request reads of possible split framed
 * {@link Transfer} payload's that comprise a single large {@link Delivery}.
 */
public class ClientReceiveContext implements ReceiveContext {

    private final ClientReceiver receiver;
    private ClientDelivery delivery;

    ClientReceiveContext(ClientReceiver receiver) {
        this.receiver = receiver;
    }

    @Override
    public Delivery delivery() {
        return delivery;
    }


    @Override
    public <E> Message<E> message() throws ClientException {
        if (delivery != null) {
            if (complete()) {
                return delivery.message();
            } else if (aborted()) {
                throw new IllegalStateException("Cannot read Message contents from an aborted message.");
            } else {
                throw new IllegalStateException("Cannot read Message contents from a partiall received message.");
            }
        } else {
            return null;
        }
    }

    @Override
    public boolean aborted() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean complete() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public RawInputStream inputStream(RawInputStreamOptions options) {
        return null;
    }
}
