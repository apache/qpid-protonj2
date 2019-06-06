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

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.proton4j.engine.util.SequenceNumber;
import org.apache.qpid.proton4j.engine.util.SplayMap;

/**
 * Tracks the incoming window and provides management of that window in relation to receiver links
 */
public class ProtonSessionIncomingWindow {

    private static final long DEFAULT_WINDOW_SIZE = Integer.MAX_VALUE; // biggest legal value

    private final ProtonSession session;
    private final ProtonEngine engine;

    // User configured incoming capacity for the session used to compute the incoming window
    private int incomingCapacity = 0;

    private long incomingWindow = 0;

    /**
     * Tracks the next expected incoming transfer ID from the remote
     */
    private long nextIncomingId = 0;

    /**
     * Tracks the most recent delivery Id for validation against the next incoming delivery
     */
    private SequenceNumber lastDeliveryid;

    private long remoteOutgoingWindow;
    private long remoteNextOutgoingId;

    private long incomingBytes;

    private SplayMap<ProtonIncomingDelivery> unsettled = new SplayMap<>();

    public ProtonSessionIncomingWindow(ProtonSession session) {
        this.session = session;
        this.engine = session.getConnection().getEngine();
    }

    public void setIncomingCapaity(int incomingCapacity) {
        this.incomingCapacity = incomingCapacity;
    }

    public int getIncomingCapacity() {
        return incomingCapacity;
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
        this.remoteNextOutgoingId = begin.getNextOutgoingId();
        this.remoteOutgoingWindow = begin.getOutgoingWindow();

        return begin;
    }

    /**
     * Update the session window state based on an incoming {@link Flow} performative
     *
     * @param flow
     *      the incoming {@link Flow} performative to process.
     */
    Flow handleFlow(Flow flow) {
        this.remoteNextOutgoingId = flow.getNextOutgoingId();
        this.remoteOutgoingWindow = flow.getOutgoingWindow();

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
        if (payload != null && !transfer.getAborted()) {
            incomingBytes += payload.getReadableBytes();
        }

        incomingWindow--;
        nextIncomingId++;

        ProtonIncomingDelivery delivery = link.remoteTransfer(transfer, payload);
        if (delivery != null && !delivery.isRemotelySettled()) {
            // TODO - An optimization for split frame deliveries would be to track
            //        if this is delivery 1 or N and only try and insert into the
            //        unsettled map on delivery 1 as the map operation can be costly.
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
        final long first = disposition.getFirst();
        final long last = disposition.hasLast() ? disposition.getLast() : first;

        if (last < first) {
            engine.engineFailed(new ProtocolViolationException(
                "Received Disposition with mismatched first and last delivery Ids: [" + first + ", " + last + "]"));
        }

        long index = first;

        // TODO - Common case will be one element so optimize for that.

        do {
            // TODO - Here is a chance for optimization, if the map containing the unsettled
            //        is navigable then we can use a sub-map to limit the range to the first
            //        and last elements and then simply walk next until the end without checking
            //        each index between for its presence.
            ProtonIncomingDelivery delivery = unsettled.get((int) index);
            if (delivery != null) {
                if (disposition.getSettled()) {
                    unsettled.remove((int) index);
                }

                delivery.getLink().remoteDisposition(disposition, delivery);
            }
        } while (++index <= last);

        return disposition;
    }

    long updateIncomingWindow() {
        // TODO - long vs int types for these unsigned value
        long maxFrameSize = session.getConnection().getMaxFrameSize();
        // TODO - need to revisit this logic and decide on sane cutoff for capacity restriction.
        if (incomingCapacity <= 0 || maxFrameSize == UnsignedInteger.MAX_VALUE.longValue()) {
            incomingWindow = DEFAULT_WINDOW_SIZE;
        } else {
            // TODO - incomingWindow = Integer.divideUnsigned(incomingCapacity - incomingBytes, maxFrameSize);
            incomingWindow = (incomingCapacity - incomingBytes) / maxFrameSize;
        }

        return incomingWindow;
    }

    void writeFlow(ProtonLink<?> link) {
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

    public long getRemoteNextOutgoingId() {
        return remoteNextOutgoingId;
    }

    public long getRemoteOutgoingWindow() {
        return remoteOutgoingWindow;
    }

    //----- Handle sender link actions in the session window context

    void processDisposition(ProtonReceiver receiver, ProtonIncomingDelivery delivery) {
        // TODO - Can we cache or pool these to not generate garbage on each send ?
        Disposition disposition = new Disposition();

        disposition.setFirst(delivery.getDeliveryId());
        disposition.setLast(delivery.getDeliveryId());
        disposition.setRole(Role.RECEIVER);
        disposition.setSettled(delivery.isSettled());
        disposition.setBatchable(false);
        disposition.setState(delivery.getLocalState());

        // TODO - Casting is ugly but our ID values are longs
        unsettled.remove((int) delivery.getDeliveryId());

        engine.pipeline().fireWrite(disposition, session.getLocalChannel(), null, null);
    }

    void deliveryRead(ProtonIncomingDelivery delivery, int bytesRead) {
        this.incomingBytes -= bytesRead;
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
