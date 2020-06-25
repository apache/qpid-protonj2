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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transport.Attach;
import org.apache.qpid.protonj2.test.driver.codec.transport.Begin;
import org.apache.qpid.protonj2.test.driver.codec.transport.Detach;
import org.apache.qpid.protonj2.test.driver.codec.transport.End;
import org.apache.qpid.protonj2.test.driver.codec.transport.Transfer;

import io.netty.buffer.ByteBuf;

/**
 * Tracks all sessions opened by the remote or initiated from the driver.
 */
public class DriverSessions {

    private final Map<UnsignedShort, SessionTracker> localSessions = new LinkedHashMap<>();
    private final Map<UnsignedShort, SessionTracker> remoteSessions = new LinkedHashMap<>();

    private final AMQPTestDriver driver;

    private short nextChannelId = 0;
    private int lastOpenedSession = -1;

    public DriverSessions(AMQPTestDriver driver) {
        this.driver = driver;
    }

    public SessionTracker getLastOpenedSession() {
        SessionTracker tracker = null;
        if (lastOpenedSession >= 0) {
            tracker = localSessions.get(UnsignedShort.valueOf(lastOpenedSession));
        }
        return tracker;
    }

    public AMQPTestDriver getDriver() {
        return driver;
    }

    public UnsignedShort getNextChannelId() {
        return UnsignedShort.valueOf(nextChannelId++);
    }

    public SessionTracker getSessionFromLocalChannel(UnsignedShort localChannel) {
        return localSessions.get(localChannel);
    }

    public SessionTracker getSessionFromRemoteChannel(UnsignedShort remoteChannel) {
        return remoteSessions.get(remoteChannel);
    }

    //----- Process Session started from the driver end

    public SessionTracker processBegin(Begin begin, int localChannel) {
        final UnsignedShort localChannelValue;
        if (localChannel < 0) {
            localChannelValue = getNextChannelId();
        } else {
            localChannelValue = UnsignedShort.valueOf(localChannel);
        }

        SessionTracker tracker = localSessions.get(localChannelValue);
        if (tracker == null) {
            tracker = new SessionTracker(driver, begin, localChannelValue, null);
            localSessions.put(tracker.getLocalChannel(), tracker);
            lastOpenedSession = localChannel;
        } else {
            // TODO - End the session with an error as we already saw this
            //        session begin and it wasn't ended yet.  End processing
            //        must be complete before doing this though.
        }

        return tracker;
    }

    public SessionTracker processLocalBegin(Begin begin, int localChannel) {
        final UnsignedShort localChannelValue;
        if (localChannel < 0) {
            localChannelValue = getNextChannelId();
        } else {
            localChannelValue = UnsignedShort.valueOf(localChannel);
        }

        // This could be a response to an inbound begin so check before
        // creating a duplicate session tracker.
        SessionTracker tracker = localSessions.get(localChannelValue);
        if (tracker == null) {
            tracker = new SessionTracker(driver, begin, localChannelValue, null);
            localSessions.put(tracker.getLocalChannel(), tracker);
            lastOpenedSession = localChannel;
        }

        return tracker;
    }

    //----- Process Session begin and end performatives

    public SessionTracker handleBegin(Begin begin, int channel) {
        final SessionTracker tracker;
        if (begin.getRemoteChannel() != null) {
            tracker = localSessions.get(begin.getRemoteChannel());
        } else {
            tracker = new SessionTracker(driver, begin, getNextChannelId(), UnsignedShort.valueOf(channel));
        }

        localSessions.put(tracker.getLocalChannel(), tracker);
        remoteSessions.put(tracker.getRemoteChannel(), tracker);
        lastOpenedSession = tracker.getLocalChannel().intValue();

        return tracker;
    }

    public SessionTracker handleEnd(End end, int channel) {
        SessionTracker tracker = remoteSessions.get(UnsignedShort.valueOf(channel));

        if (tracker != null) {
            localSessions.remove(tracker.getLocalChannel());
            remoteSessions.remove(tracker.getRemoteChannel());
        }

        return tracker;
    }

    public LinkTracker handleAttach(Attach attach, int channel) {
        SessionTracker tracker = remoteSessions.get(UnsignedShort.valueOf(channel));
        LinkTracker result = null;

        if (tracker != null) {
            result = tracker.handleAttach(attach);
        }

        return result;
    }

    public LinkTracker handleTransfer(Transfer transfer, ByteBuf payload, int channel) {
        SessionTracker tracker = remoteSessions.get(UnsignedShort.valueOf(channel));
        LinkTracker result = null;

        if (tracker != null) {
            result = tracker.handleTransfer(transfer, payload);
        }

        return result;
    }

    public LinkTracker handleDetach(Detach detach, int channel) {
        SessionTracker tracker = remoteSessions.get(UnsignedShort.valueOf(channel));
        LinkTracker result = null;

        if (tracker != null) {
            result = tracker.handleDetach(detach);
        }

        return result;
    }
}
