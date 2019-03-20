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

import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Role;

/**
 * AMQP Disposition injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public class DispositionInjectAction extends AbstractPerformativeInjectAction<Disposition> {

    private final Disposition disposition;

    public DispositionInjectAction(Disposition disposition) {
        this.disposition = disposition;
    }

    @Override
    public Disposition getPerformative() {
        return disposition;
    }

    public DispositionInjectAction withRole(Role role) {
        disposition.setRole(role);
        return this;
    }

    public DispositionInjectAction withFirst(long first) {
        disposition.setFirst(first);
        return this;
    }

    public DispositionInjectAction withLast(long last) {
        disposition.setLast(last);
        return this;
    }

    public DispositionInjectAction withSettled(boolean settled) {
        disposition.setSettled(settled);
        return this;
    }

    public DispositionInjectAction withState(DeliveryState state) {
        disposition.setState(state);
        return this;
    }

    public DispositionInjectAction withBatchable(boolean batchable) {
        disposition.setBatchable(batchable);
        return this;
    }
}
