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

import java.util.Set;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.OutgoingAMQPEnvelope;
import org.apache.qpid.protonj2.engine.util.UnsettledMap;
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

    private final ProtonSession session;
    private final ProtonEngine engine;
    private final int localChannel;

    // This is used for the delivery-id actually stamped in each transfer frame of a given message delivery.
    private int outgoingDeliveryId = 0;

    // Conceptual outgoing Transfer ID value.
    private int nextOutgoingId = 0;

    // Track outgoing windowing state information in order to stop outgoing writes if the high
    // water mark is hit and restart later once the low water mark is hit.  When outgoing capacity
    // is at the default -1 value then no real limit is applied.  If set to zero no writes are allowed.
    private int outgoingCapacity = -1;
    private int outgoingWindowHighWaterMark = Integer.MAX_VALUE;
    private int outgoingWindowLowWaterMark = Integer.MAX_VALUE / 2;
    private int pendingOutgoingWrites;
    private boolean writeable;

    private long remoteIncomingWindow;
    private int remoteNextIncomingId = nextOutgoingId;

    private Runnable outgoingFrameWriteComplete = this::handleOutgoingFrameWriteComplete;

    private final UnsettledMap<ProtonOutgoingDelivery> unsettled =
        new UnsettledMap<ProtonOutgoingDelivery>(ProtonOutgoingDelivery::getDeliveryIdInt);

    public ProtonSessionOutgoingWindow(ProtonSession session) {
        this.session = session;
        this.engine = session.getConnection().getEngine();
        this.localChannel = session.getLocalChannel();
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

        updateOutgoingWindowState();

        return begin;
    }

    int getAndIncrementNextDeliveryId() {
        return outgoingDeliveryId++;
    }

    void setOutgoingCapacity(int outgoingCapacity) {
        this.outgoingCapacity = outgoingCapacity;
        updateOutgoingWindowState();
    }

    int getOutgoingCapacity() {
        return outgoingCapacity;
    }

    int getRemainingOutgoingCapacity() {
        // If set to lower value after some writes are pending this calculation could go negative which we don't want
        // so ensure it never drops below zero.  Then limit the max value to max positive value and hold there
        // as it being more than that is a fairly pointless value to try and convey.
        final int allowedWrites = Math.max(0, outgoingWindowHighWaterMark - pendingOutgoingWrites);
        final int remaining = (int) (allowedWrites * session.getEngine().configuration().getOutboundMaxFrameSize());

        if (outgoingCapacity < 0 || remaining < 0) {
            return Integer.MAX_VALUE;
        } else {
            return remaining;
        }
    }

    boolean isSendable() {
        return writeable;
    }

    private void updateOutgoingWindowState() {
        final boolean oldWritable = writeable;
        final int maxFrameSize = (int) session.getEngine().configuration().getOutboundMaxFrameSize();

        if (outgoingCapacity == 0) {
            // At a setting of zero outgoing writes is manually disabled until elevated again to > 0
            outgoingWindowHighWaterMark = outgoingWindowLowWaterMark = 0;
            writeable = false;
        } else if (outgoingCapacity > 0) {
            // The local end is writable here if the current pending writes count is below the low water
            // mark and also if there is remote incoming window to allow more write.
            outgoingWindowHighWaterMark = Math.max(1, outgoingCapacity / maxFrameSize);
            outgoingWindowLowWaterMark = outgoingWindowHighWaterMark / 2;
            writeable = pendingOutgoingWrites <= outgoingWindowLowWaterMark && remoteIncomingWindow > 0;
        } else {
            // User disabled outgoing windowing so reset state to reflect that we are not
            // enforcing any limit from now on, at least not any sane limit.
            outgoingWindowHighWaterMark = Integer.MAX_VALUE;
            outgoingWindowLowWaterMark = Integer.MAX_VALUE / 2;
            writeable = remoteIncomingWindow > 0;
        }

        if (!oldWritable && writeable) {
            Set<ProtonSender> senders = session.senders();
            for (ProtonSender sender : senders) {
                sender.handleSessionCreditStateUpdate(this);
                if (!writeable) {
                    break;
                }
            }
        }
    }

    private void handleOutgoingFrameWriteComplete() {
        pendingOutgoingWrites = Math.max(0, --pendingOutgoingWrites);

        if (!writeable && (writeable = pendingOutgoingWrites <= outgoingWindowLowWaterMark && remoteIncomingWindow > 0)) {
            Set<ProtonSender> senders = session.senders();
            for (ProtonSender sender : senders) {
                sender.handleSessionCreditStateUpdate(this);
                if (!writeable) {
                    break;
                }
            }
        }
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

        writeable = remoteIncomingWindow > 0 && pendingOutgoingWrites <= outgoingWindowLowWaterMark;

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
            handleRangedDisposition(unsettled, disposition);
        } else {
            final ProtonOutgoingDelivery delivery = disposition.getSettled() ?
                unsettled.remove(first) : unsettled.get(first);

            if (delivery != null) {
                delivery.getLink().remoteDisposition(disposition, delivery);
            }
        }

        return disposition;
    }

    private static void handleRangedDisposition(UnsettledMap<ProtonOutgoingDelivery> unsettled, Disposition disposition) {
        // Dispositions cover a contiguous range in the map and since the tracker always moves forward
        // when appending new deliveries the range can wrap without needing a second iteration.
        if (disposition.getSettled()) {
            unsettled.removeEach((int) disposition.getFirst(), (int) disposition.getLast(), (delivery) -> {
                delivery.getLink().remoteDisposition(disposition, delivery);
            });
        } else {
            unsettled.forEach((int) disposition.getFirst(), (int) disposition.getLast(), (delivery) -> {
                delivery.getLink().remoteDisposition(disposition, delivery);
            });
        }
    }

    //----- Handle sender link actions in the session window context

    private final Disposition cachedDisposition = new Disposition();
    private final Transfer cachedTransfer = new Transfer();

    private static void handlePayloadToLargeRequiresSplitFrames(Performative performative) {
        ((Transfer) performative).setMore(true);
    }

    boolean processSend(ProtonSender sender, ProtonOutgoingDelivery delivery, ProtonBuffer payload, boolean complete) {
        // For a transfer that hasn't completed but has no bytes in the final transfer write we want
        // to allow a transfer to go out with the more flag as false.

        if (!delivery.isSettled()) {
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
                // Update session window tracking for each transfer that ends up being sent.
                ++nextOutgoingId;
                ++pendingOutgoingWrites;
                --remoteIncomingWindow;

                writeable = pendingOutgoingWrites < outgoingWindowHighWaterMark && remoteIncomingWindow > 0;

                // Only the first transfer requires the delivery tag, afterwards we can omit it for efficiency.
                if (delivery.getTransferCount() == 0) {
                    cachedTransfer.setDeliveryTag(delivery.getTag());
                } else {
                    cachedTransfer.setDeliveryTag((DeliveryTag) null);
                }
                cachedTransfer.setMore(!complete);

                OutgoingAMQPEnvelope frame = engine.wrap(cachedTransfer, localChannel, payload);

                frame.setPayloadToLargeHandler(ProtonSessionOutgoingWindow::handlePayloadToLargeRequiresSplitFrames);
                frame.setFrameWriteCompletionHandler(outgoingFrameWriteComplete);

                engine.fireWrite(frame);

                delivery.afterTransferWritten();
            } while (payload != null && payload.isReadable() && isSendable());
        } finally {
            cachedTransfer.reset();
        }

        return isSendable();
    }

    void processDisposition(ProtonSender sender, ProtonOutgoingDelivery delivery) {
        // Would only be tracked if not already remotely settled.
        if (delivery.isSettled() && !delivery.isRemotelySettled()) {
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

    int getNextOutgoingId() {
        return nextOutgoingId;
    }

    long getOutgoingWindow() {
        return Integer.MAX_VALUE;
    }

    int getRemoteNextIncomingId() {
        return remoteNextIncomingId;
    }

    long getRemoteIncomingWindow() {
        return remoteIncomingWindow;
    }
}
