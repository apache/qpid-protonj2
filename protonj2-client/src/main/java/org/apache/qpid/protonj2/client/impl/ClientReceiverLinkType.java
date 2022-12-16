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

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Future;

import org.apache.qpid.protonj2.client.Link;
import org.apache.qpid.protonj2.client.LinkOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientResourceRemotelyClosedException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for client link types that wrap a proton receiver to provide
 * delivery dispatch in some manner.
 *
 * @param <ReceiverType> The client receiver type that is being implemented.
 */
public abstract class ClientReceiverLinkType<ReceiverType extends Link<ReceiverType>> extends ClientLinkType<ReceiverType, Receiver> {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected ClientFuture<ReceiverType> drainingFuture;
    protected Future<?> drainingTimeout;

    protected Receiver protonReceiver;

    protected ClientReceiverLinkType(ClientSession session, String linkId, LinkOptions<?> options, Receiver protonReceiver) {
        super(session, linkId, options);

        this.protonReceiver = protonReceiver;
        this.protonReceiver.setLinkedResource(self());
    }

    @Override
    protected org.apache.qpid.protonj2.engine.Receiver protonLink() {
        return protonReceiver;
    }

    /**
     * Apply the given disposition and settlement state to the given incoming delivery instance.
     *
     * @param delivery
     * 		The incoming delivery that will be acted upon
     * @param state
     * 		The delivery state to apply to the given incoming delivery
     * @param settle
     * 		The settlement state to apply to the given incoming delivery
     *
     * @throws ClientException if an error occurs while applying the disposition to the delivery.
     */
    void disposition(IncomingDelivery delivery, DeliveryState state, boolean settle) throws ClientException {
        checkClosedOrFailed();
        executor.execute(() -> {
            session.getTransactionContext().disposition(delivery, state, settle);
            replenishCreditIfNeeded();
        });
    }

    //----- Abstract API that receiver must implement as they are implementation specific

    protected abstract void replenishCreditIfNeeded();

    protected abstract void handleDeliveryRead(IncomingDelivery delivery);

    //----- API that receiver may override if they need additional handling

    protected void handleDeliveryAborted(IncomingDelivery delivery) {
        LOG.trace("Delivery data was aborted: {}", delivery);
        delivery.settle();
        replenishCreditIfNeeded();
    }

    protected void handleDeliveryStateRemotelyUpdated(IncomingDelivery delivery) {
        LOG.trace("Delivery remote state was updated: {}", delivery);
    }

    protected void handleReceiverCreditUpdated(org.apache.qpid.protonj2.engine.Receiver receiver) {
        LOG.trace("Receiver credit update by remote: {}", receiver);

        if (drainingFuture != null) {
            if (receiver.getCredit() == 0) {
                drainingFuture.complete(self());
                if (drainingTimeout != null) {
                    drainingTimeout.cancel(false);
                    drainingTimeout = null;
                }
            }
        }
    }

    //----- Default receiver link handling of proton engine events

    @Override
    protected void linkSpecificLocalOpenHandler() {
        protonLink().deliveryStateUpdatedHandler(this::handleDeliveryStateRemotelyUpdated)
                    .deliveryReadHandler(this::handleDeliveryRead)
                    .deliveryAbortedHandler(this::handleDeliveryAborted)
                    .creditStateUpdateHandler(this::handleReceiverCreditUpdated);
    }

    @Override
    protected void linkSpecificRemoteOpenHandler() {
        replenishCreditIfNeeded();
    }

    @Override
    protected void linkSpecificLocalCloseHandler() {
        // Nothing needed for local close handling
    }

    @Override
    protected void linkSpecificRemoteCloseHandler() {
        // Nothing needed for receiver link remote close
    }

    @Override
    protected void linkSpecificCleanupHandler(ClientException failureCause) {
        if (drainingTimeout != null) {
            drainingFuture.failed(
                failureCause != null ? failureCause : new ClientResourceRemotelyClosedException("The Receiver has been closed"));
            drainingTimeout.cancel(false);
            drainingTimeout = null;
        }
    }
}
