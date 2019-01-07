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
package org.apache.qpid.proton4j.amqp.transport;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

public final class Disposition implements Performative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000015L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:disposition:list");

    private Role role = Role.SENDER;
    private UnsignedInteger first;
    private UnsignedInteger last;
    private boolean settled;
    private DeliveryState state;
    private boolean batchable;

    public Role getRole() {
        return role;
    }

    public void setRole(Role role) {
        if (role == null) {
            throw new NullPointerException("Role cannot be null");
        }

        this.role = role;
    }

    public UnsignedInteger getFirst() {
        return first;
    }

    public void setFirst(UnsignedInteger first) {
        if (first == null) {
            throw new NullPointerException("the first field is mandatory");
        }

        this.first = first;
    }

    public UnsignedInteger getLast() {
        return last;
    }

    public void setLast(UnsignedInteger last) {
        this.last = last;
    }

    public boolean getSettled() {
        return settled;
    }

    public void setSettled(boolean settled) {
        this.settled = settled;
    }

    public DeliveryState getState() {
        return state;
    }

    public void setState(DeliveryState state) {
        this.state = state;
    }

    public boolean getBatchable() {
        return batchable;
    }

    public void setBatchable(boolean batchable) {
        this.batchable = batchable;
    }

    @Override
    public Disposition copy() {
        Disposition copy = new Disposition();

        copy.setRole(role);
        copy.setFirst(first);
        copy.setLast(last);
        copy.setSettled(settled);
        copy.setState(state);
        copy.setBatchable(batchable);

        return copy;
    }

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.DISPOSITION;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, ProtonBuffer payload, E context) {
        handler.handleDisposition(this, payload, context);
    }

    @Override
    public String toString() {
        return "Disposition{" +
               "role=" + role +
               ", first=" + first +
               ", last=" + last +
               ", settled=" + settled +
               ", state=" + state +
               ", batchable=" + batchable +
               '}';
    }
}
