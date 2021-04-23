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

import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.StreamSender;
import org.apache.qpid.protonj2.client.StreamTracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;

/**
 * {@link StreamTracker} implementation that relies on the ClientTracker to handle the
 * basic {@link OutgoingDelivery} management.
 */
public final class ClientStreamTracker extends ClientTracker implements StreamTracker {

    ClientStreamTracker(ClientStreamSender sender, OutgoingDelivery delivery) {
        super(sender, delivery);
    }

    @Override
    public StreamSender sender() {
        return (StreamSender) super.sender();
    }

    @Override
    public StreamTracker disposition(DeliveryState state, boolean settle) throws ClientException {
        return (StreamTracker) super.disposition(state, settle);
    }

    @Override
    public StreamTracker settle() throws ClientException {
        return (StreamTracker) super.settle();
    }

    @Override
    public StreamTracker awaitSettlement() throws ClientException {
        return (StreamTracker) super.awaitSettlement();
    }

    @Override
    public StreamTracker awaitSettlement(long timeout, TimeUnit unit) throws ClientException {
        return (StreamTracker) super.awaitSettlement(timeout, unit);
    }
}
