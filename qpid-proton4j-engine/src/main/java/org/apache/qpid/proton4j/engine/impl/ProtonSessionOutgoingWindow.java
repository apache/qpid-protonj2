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

import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * Holds Session level credit window information.
 */
@SuppressWarnings("unused")
public class ProtonSessionOutgoingWindow {

    private static final long DEFAULT_WINDOW_SIZE = Integer.MAX_VALUE; // biggest legal value

    private final ProtonSession session;

    // This is used for the delivery-id actually stamped in each transfer frame of a given message delivery.
    private long outgoingDeliveryId = 0;

    // These are used for the session windows communicated via Begin/Flow frames
    // and the conceptual transfer-id relating to updating them.
    private long outgoingWindow = DEFAULT_WINDOW_SIZE;
    private long nextOutgoingId = 1;

    private long remoteIncomingWindow;
    private long remoteNextIncomingId = nextOutgoingId;

    private int outgoingBytes;

    // Obtained from the connection after the session is opened as that point in time
    // marks when this value is set in stone.
    private long maxFrameSize;

    public ProtonSessionOutgoingWindow(ProtonSession session) {
        this.session = session;
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
        maxFrameSize = session.getConnection().getMaxFrameSize();

        begin.setNextOutgoingId(nextOutgoingId);
        begin.setOutgoingWindow(outgoingWindow);

        return begin;
    }

    /**
     * Update the session level window values based on remote information.
     *
     * @param begin
     *      The {@link Begin} performative received from the remote.
     *
     * @return the given performative for chaining
     */
    Begin processInbound(Begin begin) {
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
            remoteNextIncomingId = flow.getNextIncomingId();
            remoteIncomingWindow = (flow.getNextIncomingId() + flow.getIncomingWindow()) - nextOutgoingId;
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
        return disposition;
    }

    void writeFlow(ProtonLink<?> link) {
        session.writeFlow(link);
    }

    //----- Access to internal state useful for tests

    public long getOutgoingBytes() {
        return outgoingBytes;
    }

    public long getNextOutgoingId() {
        return nextOutgoingId;
    }

    public long getOutgoingWindow() {
        return outgoingWindow;
    }

    public long getRemoteNextIncomingId() {
        return remoteNextIncomingId;
    }

    public long getRemoteIncomingWindow() {
        return remoteIncomingWindow;
    }
}
