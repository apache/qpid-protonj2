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
package org.apache.qpid.protonj2.engine.impl;

import java.util.function.Consumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.util.SplayMap;
import org.apache.qpid.protonj2.types.DeliveryTag;
import org.apache.qpid.protonj2.types.transport.Begin;
import org.apache.qpid.protonj2.types.transport.Disposition;
import org.apache.qpid.protonj2.types.transport.Flow;
import org.apache.qpid.protonj2.types.transport.Performative;
import org.apache.qpid.protonj2.types.transport.Role;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Holds Session level credit window information for outgoing transfers from this
 * Session.  The window is constrained by the remote incoming capacity restrictions
 * or if present outgoing restrictions on pending transfers.
 */
public class ProtonSessionOutgoingWindow {

    private static final int DEFAULT_WINDOW_SIZE = Integer.MAX_VALUE; // biggest legal value

    private final ProtonSession session;
    private final ProtonEngine engine;

    // This is used for the delivery-id actually stamped in each transfer frame of a given message delivery.
    private int outgoingDeliveryId = 0;

    // These are used for the session windows communicated via Begin/Flow frames
    // and the conceptual transfer-id relating to updating them.
    private long outgoingWindow = DEFAULT_WINDOW_SIZE;
    private int nextOutgoingId = 0;

    private long remoteIncomingWindow;
    private int remoteNextIncomingId = nextOutgoingId;

    private int outgoingBytes;

    private final SplayMap<ProtonOutgoingDelivery> unsettled = new SplayMap<>();

    public ProtonSessionOutgoingWindow(ProtonSession session) {
        this.session = session;
        this.engine = session.getConnection().getEngine();
    }

    /**
     * Initialize the session level window values on the outbound Begin
     *
     * @param begin
     *      The {@link Begin} performative that is about to be sent.
     *
     * @return the configured performative
     */
    Begin configureOutbound(Begin begin) {
        begin.setNextOutgoingId(getNextOutgoingId());
        begin.setOutgoingWindow(getOutgoingWindow());

        return begin;
    }

    int getAndIncrementNextDeliveryId() {
        return outgoingDeliveryId++;
    }

    //----- Handle incoming performatives relevant to the session.

    /**
     * Update the session level window values based on remote information.
     *
     * @param begin
     *      The {@link Begin} performative received from the remote.
     *
     * @return the given performative for chaining
     */
    Begin handleBegin(Begin begin) {
        remoteIncomingWindow = begin.getIncomingWindow();
        return begin;
    }

    /**
     * Update the session window state based on an incoming {@link Flow} performative
     *
     * @param flow
     *      the incoming {@link Flow} performative to process.
     */
    Flow handleFlow(Flow flow) {
        if (flow.hasNextIncomingId()) {
            remoteNextIncomingId = (int) flow.getNextIncomingId();
            remoteIncomingWindow = (remoteNextIncomingId + flow.getIncomingWindow()) - nextOutgoingId;
        } else {
            remoteIncomingWindow = flow.getIncomingWindow();
        }

        return flow;
    }

    /**
     * Update the session window state based on an incoming {@link Transfer} performative
     *
     * @param transfer
     *      the incoming {@link Transfer} performative to process.
     */
    Transfer handleTransfer(Transfer transfer, ProtonBuffer payload) {
        return transfer;
    }

    /**
     * Update the state of any sent Transfers that are indicated in the disposition
     * with the state information conveyed therein.
     *
     * @param disposition
     *      The {@link Disposition} performative to process
     *
     * @return the {@link Disposition}
     */
    Disposition handleDisposition(Disposition disposition) {
        final int first = (int) disposition.getFirst();

        if (disposition.hasLast() && disposition.getLast() != first) {
            handleRangedDisposition(disposition);
        } else {
            final ProtonOutgoingDelivery delivery = disposition.getSettled() ?
                unsettled.remove(first) : unsettled.get(first);

            if (delivery != null) {
                delivery.getLink().remoteDisposition(disposition, delivery);
            }
        }

        return disposition;
    }

    private void handleRangedDisposition(Disposition disposition) {
        final int first = (int) disposition.getFirst();
        final int last = (int) disposition.getLast();
        final boolean settled = disposition.getSettled();

        int index = first;
        ProtonOutgoingDelivery delivery;

        // TODO: If SplayMap gets a subMap that works we could get the ranged view which would
        //       be more efficient.
        do {
            delivery = settled ? unsettled.remove(index) : unsettled.get(index);

            if (delivery != null) {
                delivery.getLink().remoteDisposition(disposition, delivery);
            }
        } while (index++ != last);
    }

    void writeFlow(ProtonSender link) {
        session.writeFlow(link);
    }

    //----- Handle sender link actions in the session window context

    private final Disposition cachedDisposition = new Disposition();
    private final Transfer cachedTransfer = new Transfer();
    private final Consumer<Performative> cachedTransferUpdater = (performative) -> cachedTransfer.setMore(true);

    void processSend(ProtonSender sender, ProtonOutgoingDelivery delivery, ProtonBuffer payload, boolean complete) {
        // For a transfer that hasn't completed but has no bytes in the final transfer write we want
        // to allow a transfer to go out with the more flag as false.

        if (!delivery.isSettled()) {
            // TODO - Casting is ugly
            unsettled.put((int) delivery.getDeliveryId(), delivery);
        }

        try {
            cachedTransfer.setDeliveryId(delivery.getDeliveryId());
            if (delivery.getMessageFormat() != 0) {
                cachedTransfer.setMessageFormat(delivery.getMessageFormat());
            } else {
                cachedTransfer.clearMessageFormat();
            }
            cachedTransfer.setHandle(sender.getHandle());
            cachedTransfer.setSettled(delivery.isSettled());
            cachedTransfer.setState(delivery.getState());

            do {
                // Only the first transfer requires the delivery tag, afterwards we can omit it for efficiency.
                if (delivery.getTransferCount() == 0) {
                    cachedTransfer.setDeliveryTag(delivery.getTag());
                } else {
                    cachedTransfer.setDeliveryTag((DeliveryTag) null);
                }
                cachedTransfer.setMore(!complete);

                try {
                    engine.fireWrite(engine.wrap(cachedTransfer, session.getLocalChannel(), payload).setPayloadToLargeHandler(cachedTransferUpdater));
                } finally {
                    delivery.afterTransferWritten();
                }

                // Update session window tracking
                nextOutgoingId++;
                remoteIncomingWindow--;
            } while (payload != null && payload.isReadable() && remoteIncomingWindow > 0);
        } finally {
            cachedTransfer.reset();
        }
    }

    void processDisposition(ProtonSender sender, ProtonOutgoingDelivery delivery) {
        // Would only be tracked if not already remotely settled.
        if (delivery.isSettled() && !delivery.isRemotelySettled()) {
            // TODO - Casting is ugly but our ID values are longs
            unsettled.remove((int) delivery.getDeliveryId());
        }

        if (!delivery.isRemotelySettled()) {
            cachedDisposition.setFirst(delivery.getDeliveryId());
            cachedDisposition.setRole(Role.SENDER);
            cachedDisposition.setSettled(delivery.isSettled());
            cachedDisposition.setState(delivery.getState());

            try {
                engine.fireWrite(cachedDisposition, session.getLocalChannel());
            } finally {
                cachedDisposition.reset();
            }
        }
    }

    void processAbort(ProtonSender sender, ProtonOutgoingDelivery delivery) {
        cachedTransfer.setDeliveryId(delivery.getDeliveryId());
        cachedTransfer.setDeliveryTag(delivery.getTag());
        cachedTransfer.setSettled(true);
        cachedTransfer.setAborted(true);
        cachedTransfer.setHandle(sender.getHandle());

        // Ensure we don't track the aborted delivery any longer.
        unsettled.remove((int) delivery.getDeliveryId());

        try {
            engine.fireWrite(cachedTransfer, session.getLocalChannel());
        } finally {
            cachedTransfer.reset();
        }
    }

    //----- Access to internal state useful for tests

    long getOutgoingBytes() {
        return outgoingBytes;
    }

    int getNextOutgoingId() {
        return nextOutgoingId;
    }

    long getOutgoingWindow() {
        return outgoingWindow;
    }

    int getRemoteNextIncomingId() {
        return remoteNextIncomingId;
    }

    long getRemoteIncomingWindow() {
        return remoteIncomingWindow;
    }
}
