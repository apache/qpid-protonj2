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

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.protonj2.engine.util.SequenceNumber;
import org.apache.qpid.protonj2.engine.util.SplayMap;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.transport.Begin;
import org.apache.qpid.protonj2.types.transport.Disposition;
import org.apache.qpid.protonj2.types.transport.Flow;
import org.apache.qpid.protonj2.types.transport.Role;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Tracks the incoming window and provides management of that window in relation to receiver links.
 * <p>
 * The incoming window decreases as {@link Transfer} frames arrive and is replenished when the user reads the
 * bytes received in the accumulated payload of a delivery.  The window is expanded by sending a {@link Flow}
 * frame to the remote with an updated incoming window value at configured intervals based on reads from the
 * pending deliveries.
 */
public class ProtonSessionIncomingWindow {

    private static final long DEFAULT_WINDOW_SIZE = Integer.MAX_VALUE; // biggest legal value

    private final ProtonSession session;
    private final ProtonEngine engine;

    // User configured incoming capacity for the session used to compute the incoming window
    private int incomingCapacity = 0;

    // Computed incoming window based on the incoming capacity minus bytes not yet read from deliveries.
    private long incomingWindow = 0;

    // Tracks the next expected incoming transfer ID from the remote
    private long nextIncomingId = 0;

    // Tracks the most recent delivery Id for validation against the next incoming delivery
    private SequenceNumber lastDeliveryid;

    private long maxFrameSize;
    private long incomingBytes;

    private SplayMap<ProtonIncomingDelivery> unsettled = new SplayMap<>();

    public ProtonSessionIncomingWindow(ProtonSession session) {
        this.session = session;
        this.engine = session.getConnection().getEngine();
        this.maxFrameSize = session.getConnection().getMaxFrameSize();
    }

    public void setIncomingCapacity(int incomingCapacity) {
        this.incomingCapacity = incomingCapacity;
    }

    public int getIncomingCapacity() {
        return incomingCapacity;
    }

    public int getRemainingIncomingCapacity() {
        // TODO: This is linked to below update of capacity which also needs more attention.
        if (incomingCapacity <= 0 || maxFrameSize == UnsignedInteger.MAX_VALUE.longValue()) {
            return (int) DEFAULT_WINDOW_SIZE;
        } else {
            return (int) (incomingCapacity - incomingBytes);
        }
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
        // Update as it might have changed if session created before connection open() called.
        this.maxFrameSize = session.getConnection().getMaxFrameSize();

        return begin.setIncomingWindow(updateIncomingWindow());
    }

    /**
     * Update the session level window values based on remote information.
     *
     * @param begin
     *      The {@link Begin} performative received from the remote.
     *
     * @return the given performative for chaining
     */
    Begin handleBegin(Begin begin) {
        if (begin.hasNextOutgoingId()) {
            this.nextIncomingId = begin.getNextOutgoingId();
        }

        return begin;
    }

    /**
     * Update the session window state based on an incoming {@link Flow} performative
     *
     * @param flow
     *      the incoming {@link Flow} performative to process.
     */
    Flow handleFlow(Flow flow) {
        return flow;
    }

    /**
     * Update the session window state based on an incoming {@link Transfer} performative
     *
     * @param transfer
     *      the incoming {@link Transfer} performative to process.
     * @param payload
     *      the payload that was transmitted with the incoming {@link Transfer}
     */
    Transfer handleTransfer(ProtonLink<?> link, Transfer transfer, ProtonBuffer payload) {
        incomingBytes += payload != null ? payload.getReadableBytes() : 0;
        incomingWindow--;
        nextIncomingId++;

        ProtonIncomingDelivery delivery = link.remoteTransfer(transfer, payload);
        if (!delivery.isSettled() && !delivery.isRemotelySettled() && delivery.isFirstTransfer()) {
            unsettled.put((int) delivery.getDeliveryId(), delivery);
        }

        return transfer;
    }

    /**
     * Update the state of any received Transfers that are indicated in the disposition
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
            final ProtonIncomingDelivery delivery = disposition.getSettled() ?
                unsettled.remove(first) : unsettled.get(first);

            if (delivery != null) {
                delivery.getLink().remoteDisposition(disposition, delivery);
            }
        }

        return disposition;
    }

    private static void handleRangedDisposition(NavigableMap<UnsignedInteger, ProtonIncomingDelivery> unsettled, Disposition disposition) {
        final UnsignedInteger first = UnsignedInteger.valueOf(disposition.getFirst());
        final UnsignedInteger last = UnsignedInteger.valueOf(disposition.getLast());

        // Dispositions cover a contiguous range in the map requires a single sub-map
        // which we can iterate whereas a range that wraps requires two iterations over
        // a split between the higher portion and the lower portion of the map.
        if (first.compareTo(last) <= 0) {
            handleDispositions(unsettled.subMap(first, true, last, true), disposition);
        } else {
            handleDispositions(unsettled.tailMap(first, true), disposition);
            handleDispositions(unsettled.headMap(last, true), disposition);
        }
    }

    private static void handleDispositions(Map<UnsignedInteger, ProtonIncomingDelivery> deliveries, Disposition disposition) {
        final boolean settled = disposition.getSettled();

        Iterator<ProtonIncomingDelivery> deliveriesIter = deliveries.values().iterator();
        while (deliveriesIter.hasNext()) {
            ProtonIncomingDelivery delivery = deliveriesIter.next();

            if (settled) {
                deliveriesIter.remove();
            }

            delivery.getLink().remoteDisposition(disposition, delivery);
        }
    }

    long updateIncomingWindow() {
        // TODO - need to revisit this logic and decide on sane cutoff for capacity restriction.
        if (incomingCapacity <= 0 || maxFrameSize == UnsignedInteger.MAX_VALUE.longValue()) {
            incomingWindow = DEFAULT_WINDOW_SIZE;
        } else {
            // TODO - incomingWindow = Integer.divideUnsigned(incomingCapacity - incomingBytes, maxFrameSize);
            incomingWindow = (incomingCapacity - incomingBytes) / maxFrameSize;
        }

        return incomingWindow;
    }

    void writeFlow(ProtonReceiver link) {
        updateIncomingWindow();
        session.writeFlow(link);
    }

    //----- Access to internal state useful for tests

    public long getIncomingBytes() {
        return incomingBytes;
    }

    public long getNextIncomingId() {
        return nextIncomingId;
    }

    public long getIncomingWindow() {
        return incomingWindow;
    }

    //----- Handle sender link actions in the session window context

    private final Disposition cachedDisposition = new Disposition();

    void processDisposition(ProtonReceiver receiver, ProtonIncomingDelivery delivery) {
        if (!delivery.isRemotelySettled()) {
            // Would only be tracked if not already remotely settled.
            if (delivery.isSettled()) {
                unsettled.remove((int) delivery.getDeliveryId());
            }

            cachedDisposition.reset();
            cachedDisposition.setFirst(delivery.getDeliveryId());
            cachedDisposition.setRole(Role.RECEIVER);
            cachedDisposition.setSettled(delivery.isSettled());
            cachedDisposition.setState(delivery.getState());

            engine.fireWrite(cachedDisposition, session.getLocalChannel());
        }
    }

    void deliveryRead(ProtonIncomingDelivery delivery, int bytesRead) {
        this.incomingBytes -= bytesRead;
        if (incomingWindow == 0) {
            writeFlow(delivery.getLink());
        }
    }

    void validateNextDeliveryId(long deliveryId) {
        if (lastDeliveryid == null) {
            lastDeliveryid = new SequenceNumber((int) deliveryId);
        } else {
            int previousId = lastDeliveryid.intValue();
            if (lastDeliveryid.increment().compareTo((int) deliveryId) != 0) {
                session.getConnection().getEngine().engineFailed(
                    new ProtocolViolationException("Expected delivery-id " + previousId + ", got " + deliveryId));
            }
        }
    }
}
