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

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.futures.ClientFutureFactory;

/**
 * A dummy Tracker instance that always indicates remote settlement and
 * acceptance.
 */
public class ClientNoOpTracker implements Tracker {

    private final ClientSender sender;

    private DeliveryState state;
    private boolean settled;

    public ClientNoOpTracker(ClientSender sender) {
        this.sender = sender;
    }

    @Override
    public Sender sender() {
        return sender;
    }

    @Override
    public Tracker settle() throws ClientException {
        this.settled = true;
        return this;
    }

    @Override
    public boolean settled() {
        return settled;
    }

    @Override
    public DeliveryState state() {
        return state;
    }

    @Override
    public Tracker disposition(DeliveryState state, boolean settle) throws ClientException {
        this.state = state;
        this.settled = settle;

        return this;
    }

    @Override
    public DeliveryState remoteState() {
        return ClientDeliveryState.ClientAccepted.getInstance();
    }

    @Override
    public boolean remoteSettled() {
        return true;
    }

    @Override
    public Future<Tracker> settlementFuture() {
        return ClientFutureFactory.completedFuture(this);
    }

    @Override
    public Tracker awaitSettlement() throws ClientException {
        return this;
    }

    @Override
    public Tracker awaitSettlement(long timeout, TimeUnit unit) throws ClientException {
        return this;
    }
}
