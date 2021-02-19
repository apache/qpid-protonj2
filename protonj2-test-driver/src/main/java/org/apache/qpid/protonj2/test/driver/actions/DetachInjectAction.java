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
package org.apache.qpid.protonj2.test.driver.actions;

import java.util.Map;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.SessionTracker;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transport.Detach;
import org.apache.qpid.protonj2.test.driver.codec.transport.ErrorCondition;
import org.apache.qpid.protonj2.test.driver.codec.util.TypeMapper;

/**
 * AMQP Detach injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public class DetachInjectAction extends AbstractPerformativeInjectAction<Detach> {

    private final Detach detach = new Detach();

    public DetachInjectAction(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public Detach getPerformative() {
        return detach;
    }

    public DetachInjectAction withHandle(int handle) {
        detach.setHandle(UnsignedInteger.valueOf(handle));
        return this;
    }

    public DetachInjectAction withHandle(long handle) {
        detach.setHandle(UnsignedInteger.valueOf(handle));
        return this;
    }

    public DetachInjectAction withHandle(UnsignedInteger handle) {
        detach.setHandle(handle);
        return this;
    }

    public DetachInjectAction withClosed(boolean closed) {
        detach.setClosed(closed);
        return this;
    }

    public DetachInjectAction withClosed(Boolean closed) {
        detach.setClosed(closed);
        return this;
    }

    public DetachInjectAction withErrorCondition(ErrorCondition error) {
        detach.setError(error);
        return this;
    }

    public DetachInjectAction withErrorCondition(String condition, String description) {
        detach.setError(new ErrorCondition(Symbol.valueOf(condition), description));
        return this;
    }

    public DetachInjectAction withErrorCondition(Symbol condition, String description) {
        detach.setError(new ErrorCondition(condition, description));
        return this;
    }

    public DetachInjectAction withErrorCondition(String condition, String description, Map<String, Object> info) {
        detach.setError(new ErrorCondition(Symbol.valueOf(condition), description, TypeMapper.toSymbolKeyedMap(info)));
        return this;
    }

    public DetachInjectAction withErrorCondition(Symbol condition, String description, Map<Symbol, Object> info) {
        detach.setError(new ErrorCondition(condition, description, info));
        return this;
    }

    @Override
    protected void beforeActionPerformed(AMQPTestDriver driver) {
        // A test that is trying to send an unsolicited detach must provide a channel as we
        // won't attempt to make up one since we aren't sure what the intent here is.
        if (onChannel() == CHANNEL_UNSET) {
            if (driver.sessions().getLastLocallyOpenedSession() == null) {
                throw new AssertionError("Scripted Action cannot run without a configured channel: " +
                                         "No locally opened session exists to auto select a channel.");
            }

            onChannel(driver.sessions().getLastLocallyOpenedSession().getLocalChannel().intValue());
        }

        final UnsignedShort localChannel = UnsignedShort.valueOf(onChannel());
        final SessionTracker session = driver.sessions().getSessionFromLocalChannel(localChannel);

        // A test might be trying to send Attach outside of session scope to check for error handling
        // of unexpected performatives so we just allow no session cases and send what we are told.
        if (session != null) {
            // Auto select last opened sender on last opened session.  Later an option could
            // be added to allow forcing the handle to be null for testing specification requirements.
            if (detach.getHandle() == null) {
                detach.setHandle(session.getLastOpenedLink().getHandle());
            }

            session.handleLocalDetach(detach);
        }
    }
}
