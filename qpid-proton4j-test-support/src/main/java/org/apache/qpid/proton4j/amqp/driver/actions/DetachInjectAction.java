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
package org.apache.qpid.proton4j.amqp.driver.actions;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Detach;
import org.apache.qpid.proton4j.amqp.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;

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

    public DetachInjectAction withErrorCondition(ErrorCondition error) {
        detach.setError(TypeMapper.mapFromProtonType(error));
        return this;
    }

    @Override
    protected void beforeActionPerformed(AMQPTestDriver driver) {
        // We fill in a channel using the next available channel id if one isn't set, then
        // report the outbound begin to the session so it can track this new session.
        if (onChannel() == CHANNEL_UNSET) {
            onChannel(driver.getSessions().getLastOpenedSession().getLocalChannel().intValue());
        }

        // Auto select last opened sender on last opened session.  Later an option could
        // be added to allow forcing the handle to be null for testing specification requirements.
        if (detach.getHandle() == null) {
            detach.setHandle(driver.getSessions().getLastOpenedSession().getLastOpenedLink().getHandle());
        }

        // TODO - Process detach in the local side of the link when needed for added validation
    }
}
