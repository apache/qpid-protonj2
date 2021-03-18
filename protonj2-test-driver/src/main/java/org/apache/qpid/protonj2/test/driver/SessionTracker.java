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
import org.apache.qpid.protonj2.test.driver.codec.transport.Flow;
import org.apache.qpid.protonj2.test.driver.codec.transport.Role;
import org.apache.qpid.protonj2.test.driver.codec.transport.Transfer;

import io.netty.buffer.ByteBuf;

/**
 * Tracks information related to an opened Session and its various links
 */
public class SessionTracker {

    private final Deque<LinkTracker> remoteSenders = new ArrayDeque<>();
    private final Deque<LinkTracker> remoteReceivers = new ArrayDeque<>();

    private final Map<UnsignedInteger, LinkTracker> trackerMap = new LinkedHashMap<>();

    private UnsignedShort localChannel;
    private UnsignedShort remoteChannel;
    private UnsignedInteger nextIncomingId;
    private UnsignedInteger nextOutgoingId;
    private Begin remoteBegin;
    private Begin localBegin;
    private End remoteEnd;
    private End localEnd;
    private LinkTracker lastOpenedLink;
    private LinkTracker lastOpenedCoordinatorLink;

    private final AMQPTestDriver driver;

    public SessionTracker(AMQPTestDriver driver) {
        this.driver = driver;
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

    public LinkTracker getLastOpenedRemoteSender() {
        return remoteSenders.getLast();
    }

    public LinkTracker getLastOpenedRemoteReceiver() {
        return remoteReceivers.getLast();
    }

    public End getRemoteEnd() {
        return remoteEnd;
    }

    public End getLocalEnd() {
        return localEnd;
    }

    //----- Session specific access which can provide details for expectations

    public Begin getRemoteBegin() {
        return remoteBegin;
    }

    public Begin getLocalBegin() {
        return localBegin;
    }

    public UnsignedShort getRemoteChannel() {
        return remoteChannel;
    }

    public UnsignedShort getLocalChannel() {
        return localChannel;
    }

    public UnsignedInteger getNextIncomingId() {
        return nextIncomingId;
    }

    public UnsignedInteger getNextOutgoingId() {
        return nextOutgoingId;
    }

    //----- Handle performatives and update session state

    public SessionTracker handleBegin(Begin remoteBegin, UnsignedShort remoteChannel) {
        this.remoteBegin = remoteBegin;
        this.remoteChannel = remoteChannel;
        this.nextIncomingId = remoteBegin.getNextOutgoingId();

        return this;
    }

    public SessionTracker handleLocalBegin(Begin localBegin, UnsignedShort localChannel) {
        this.localBegin = localBegin;
        this.localChannel = localChannel;
        this.nextOutgoingId = localBegin.getNextOutgoingId();

        return this;
    }

    public SessionTracker handleEnd(End end) {
        this.remoteEnd = end;
        return this;
    }

    public SessionTracker handleLocalEnd(End end) {
        this.localEnd = end;
        return this;
    }

    public LinkTracker handleRemoteAttach(Attach attach) {
        LinkTracker linkTracker = trackerMap.get(attach.getHandle());

        // We only populate these remote value here, never in the local side processing
        // this implies that we need to check if this was remotely initiated and create
        // the link tracker if none exists yet
        // TODO: These SenderTracker and ReceiverTracker inversions are confusing and probably
        //       not going to work for future enhancements.
        if (attach.getRole().equals(Role.SENDER.getValue())) {
            if (linkTracker == null) {
                linkTracker = new ReceiverTracker(this, attach);
            }
            remoteSenders.add(linkTracker);
        } else {
            if (linkTracker == null) {
                linkTracker = new SenderTracker(this, attach);
            }
            remoteReceivers.add(linkTracker);
        }

        if (attach.getTarget() instanceof Coordinator) {
            lastOpenedCoordinatorLink = linkTracker;
            driver.sessions().setLastOpenedCoordinator(lastOpenedCoordinatorLink);
        }

        lastOpenedLink = linkTracker;
        trackerMap.put(attach.getHandle(), linkTracker);

        return linkTracker;
    }

    public LinkTracker handleLocalAttach(Attach attach) {
        LinkTracker linkTracker = trackerMap.get(attach.getHandle());

        // Create a tracker for the local side to use to respond to remote
        // performative or to use when invoking local actions.
        if (linkTracker == null) {
            if (attach.getRole().equals(Role.SENDER.getValue())) {
                linkTracker = new SenderTracker(this, attach);
            } else {
                linkTracker = new ReceiverTracker(this, attach);
            }
        }

        lastOpenedLink = linkTracker;
        trackerMap.put(attach.getHandle(), linkTracker);

        return linkTracker;
    }

    public LinkTracker handleRemoteDetach(Detach detach) {
        LinkTracker tracker = trackerMap.get(detach.getHandle());

        if (tracker != null) {
            remoteSenders.remove(tracker);
            remoteReceivers.remove(tracker);
        }

        return tracker;
    }

    public LinkTracker handleLocalDetach(Detach detach) {
        LinkTracker tracker = trackerMap.get(detach.getHandle());

        // TODO: Cleanup local state when we start tracking both sides.

        return tracker;
    }

    public LinkTracker handleTransfer(Transfer transfer, ByteBuf payload) {
        LinkTracker tracker = trackerMap.get(transfer.getHandle());

        tracker.handleTransfer(transfer, payload);
        // TODO - Update session state based on transfer

        return tracker;
    }

    public LinkTracker handleFlow(Flow flow) {
        LinkTracker tracker = null;

        if (flow.getHandle() != null) {
            tracker = trackerMap.get(flow.getHandle());
            tracker.handleFlow(flow);
        }

        return tracker;
    }

    public UnsignedInteger findFreeLocalHandle() {
        final UnsignedInteger HANDLE_MAX = localBegin.getHandleMax() != null ? localBegin.getHandleMax() : UnsignedInteger.MAX_VALUE;

        for (long i = 0; i <= HANDLE_MAX.longValue(); ++i) {
            final UnsignedInteger handle = UnsignedInteger.valueOf(i);
            if (!trackerMap.containsKey(handle)) {
                return handle;
            }
        }

        throw new IllegalStateException("no local handle available for allocation");
    }
}
