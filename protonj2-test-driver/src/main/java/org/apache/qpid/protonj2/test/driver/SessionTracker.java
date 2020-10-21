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
package org.apache.qpid.protonj2.test.driver;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transactions.Coordinator;
import org.apache.qpid.protonj2.test.driver.codec.transport.Attach;
import org.apache.qpid.protonj2.test.driver.codec.transport.Begin;
import org.apache.qpid.protonj2.test.driver.codec.transport.Detach;
import org.apache.qpid.protonj2.test.driver.codec.transport.End;
import org.apache.qpid.protonj2.test.driver.codec.transport.Role;
import org.apache.qpid.protonj2.test.driver.codec.transport.Transfer;

import io.netty.buffer.ByteBuf;

/**
 * Tracks information related to an opened Session and its various links
 */
public class SessionTracker {

    private static final UnsignedInteger DEFAULT_WINDOW_SIZE = UnsignedInteger.valueOf(Integer.MAX_VALUE);

    private final Deque<LinkTracker> senders = new ArrayDeque<>();
    private final Deque<LinkTracker> receivers = new ArrayDeque<>();

    private final Map<UnsignedInteger, LinkTracker> trackerMap = new LinkedHashMap<>();

    private final Begin begin;
    private final UnsignedShort localChannel;
    private final UnsignedShort remoteChannel;

    private UnsignedInteger nextOutgoingId = UnsignedInteger.ONE;
    private UnsignedInteger nextIncomingId = UnsignedInteger.ONE;
    private UnsignedInteger incomingWindow = DEFAULT_WINDOW_SIZE;
    private UnsignedInteger outgoingWindow = DEFAULT_WINDOW_SIZE;
    private UnsignedInteger handleMax;
    private End end;
    private LinkTracker lastOpenedLink;
    private LinkTracker lastOpenedCoordinatorLink;

    private final AMQPTestDriver driver;

    public SessionTracker(AMQPTestDriver driver, Begin begin, UnsignedShort localChannel, UnsignedShort remoteChannel) {
        this.driver = driver;
        this.begin = begin;
        this.localChannel = localChannel;
        this.remoteChannel = remoteChannel;
        this.nextIncomingId = begin.getNextOutgoingId();
    }

    public AMQPTestDriver getDriver() {
        return driver;
    }

    public LinkTracker getLastOpenedLink() {
        return lastOpenedLink;
    }

    public LinkTracker getLastOpenedCoordinatorLink() {
        return lastOpenedCoordinatorLink;
    }

    public LinkTracker getLastOpenedSender() {
        return senders.getLast();
    }

    public LinkTracker getLastOpenedReceiver() {
        return receivers.getLast();
    }

    public End getEnd() {
        return end;
    }

    //----- Session specific access which can provide details for expectations

    public Begin getRemoteBegin() {
        return begin;
    }

    public UnsignedShort getRemoteChannel() {
        return remoteChannel;
    }

    public UnsignedShort getLocalChannel() {
        return localChannel;
    }

    public UnsignedInteger getNextOutgoingId() {
        return nextOutgoingId;
    }

    public UnsignedInteger setNextOutgoingId(UnsignedInteger nextOutgoingId) {
        this.nextOutgoingId = nextOutgoingId;
        return nextOutgoingId;
    }

    public UnsignedInteger getNextIncomingId() {
        return nextIncomingId;
    }

    public UnsignedInteger setNextIncomingId(UnsignedInteger nextIncomingId) {
        this.nextIncomingId = nextIncomingId;
        return nextIncomingId;
    }

    public UnsignedInteger getIncomingWindow() {
        return incomingWindow;
    }

    public UnsignedInteger setIncomingWindow(UnsignedInteger incomingWindow) {
        this.incomingWindow = incomingWindow;
        return incomingWindow;
    }

    public UnsignedInteger getOutgoingWindow() {
        return outgoingWindow;
    }

    public UnsignedInteger setOutgoingWindow(UnsignedInteger outgoingWindow) {
        this.outgoingWindow = outgoingWindow;
        return outgoingWindow;
    }

    public UnsignedInteger getHandleMax() {
        return handleMax;
    }

    public UnsignedInteger setHandleMax(UnsignedInteger handleMax) {
        this.handleMax = handleMax;
        return handleMax;
    }

    //----- Handle performatives and update session state

    public SessionTracker handleEnd(End end) {
        this.end = end;
        return this;
    }

    public LinkTracker handleAttach(Attach attach) {
        LinkTracker linkTracker = new LinkTracker(this, attach);

        if (attach.getRole().equals(Role.SENDER.getValue())) {
            senders.add(linkTracker);
        } else {
            receivers.add(linkTracker);
        }

        if (attach.getTarget() instanceof Coordinator) {
            lastOpenedCoordinatorLink = linkTracker;
            driver.getSessions().setLastOpenedCoordinator(lastOpenedCoordinatorLink);
        }

        lastOpenedLink = linkTracker;

        trackerMap.put(attach.getHandle(), linkTracker);

        return linkTracker;
    }

    public LinkTracker handleDetach(Detach detach) {
        LinkTracker tracker = trackerMap.get(detach.getHandle());

        if (tracker != null) {
            senders.remove(tracker);
            receivers.remove(tracker);
        }

        return tracker;
    }

    public LinkTracker handleTransfer(Transfer transfer, ByteBuf payload) {
        LinkTracker tracker = trackerMap.get(transfer.getHandle());

        // TODO - Update session state based on transfer

        return tracker;
    }
}
