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
package org.apache.qpid.proton4j.amqp.driver;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Attach;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Begin;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Detach;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.End;

/**
 * Tracks all sessions opened by the remote or initiated from the driver.
 */
public class DriverSessions {

    private final Map<UnsignedShort, SessionTracker> localSessions = new LinkedHashMap<>();
    private final Map<UnsignedShort, SessionTracker> remoteSessions = new LinkedHashMap<>();

    private final AMQPTestDriver driver;

    private short nextChannelId = 0;

    public DriverSessions(AMQPTestDriver driver) {
        this.driver = driver;
    }

    public SessionTracker getLastOpenedSession() {
        return null; // TODO
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

        SessionTracker tracker = new SessionTracker(driver, begin, localChannelValue, null);

        localSessions.put(tracker.getLocalChannel(), tracker);

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

    public LinkTracker handleDetach(Detach detach, int channel) {
        SessionTracker tracker = remoteSessions.get(UnsignedShort.valueOf(channel));
        LinkTracker result = null;

        if (tracker != null) {
            result = tracker.handleDetach(detach);
        }

        return result;
    }
}
