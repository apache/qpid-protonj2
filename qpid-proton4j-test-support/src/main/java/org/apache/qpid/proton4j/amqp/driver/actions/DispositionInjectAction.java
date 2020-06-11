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

import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Disposition;
import org.apache.qpid.proton4j.amqp.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.types.UnsignedInteger;
import org.apache.qpid.proton4j.types.transport.DeliveryState;
import org.apache.qpid.proton4j.types.transport.Role;

/**
 * AMQP Disposition injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public class DispositionInjectAction extends AbstractPerformativeInjectAction<Disposition> {

    private final Disposition disposition = new Disposition();

    public DispositionInjectAction(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public Disposition getPerformative() {
        return disposition;
    }

    public DispositionInjectAction withRole(Role role) {
        disposition.setRole(role.getValue());
        return this;
    }

    public DispositionInjectAction withFirst(long first) {
        disposition.setFirst(UnsignedInteger.valueOf(first));
        return this;
    }

    public DispositionInjectAction withFirst(UnsignedInteger first) {
        disposition.setFirst(first);
        return this;
    }

    public DispositionInjectAction withLast(long last) {
        disposition.setLast(UnsignedInteger.valueOf(last));
        return this;
    }

    public DispositionInjectAction withLast(UnsignedInteger last) {
        disposition.setLast(last);
        return this;
    }

    public DispositionInjectAction withSettled(boolean settled) {
        disposition.setSettled(settled);
        return this;
    }

    public DispositionInjectAction withState(DeliveryState state) {
        disposition.setState(TypeMapper.mapFromProtonType(state));
        return this;
    }

    public DispositionInjectAction withBatchable(boolean batchable) {
        disposition.setBatchable(batchable);
        return this;
    }

    @Override
    protected void beforeActionPerformed(AMQPTestDriver driver) {
        // We fill in a channel using the next available channel id if one isn't set, then
        // report the outbound begin to the session so it can track this new session.
        if (onChannel() == CHANNEL_UNSET) {
            onChannel(driver.getSessions().getLastOpenedSession().getLocalChannel().intValue());
        }

        // TODO - Process disposition in the local side of the link when needed for added validation
    }
}
