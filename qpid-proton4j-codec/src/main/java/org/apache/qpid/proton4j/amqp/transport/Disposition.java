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
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

public final class Disposition implements Performative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000015L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:disposition:list");

    private static final long UINT_MAX = 0xFFFFFFFFL;

    private static int ROLE = 1;
    private static int FIRST = 2;
    private static int LAST = 4;
    private static int SETTLED = 8;
    private static int STATE = 16;
    private static int BATCHABLE = 32;

    private int modified = 0;

    private Role role = Role.SENDER;
    private long first;
    private long last;
    private boolean settled;
    private DeliveryState state;
    private boolean batchable;

    //----- Query the state of the Header object -----------------------------//

    public boolean isEmpty() {
        return modified == 0;
    }

    public int getElementCount() {
        return 32 - Integer.numberOfLeadingZeros(modified);
    }

    public boolean hasRole() {
        return (modified & ROLE) == ROLE;
    }

    public boolean hasFirst() {
        return (modified & FIRST) == FIRST;
    }

    public boolean hasLast() {
        return (modified & LAST) == LAST;
    }

    public boolean hasSettled() {
        return (modified & SETTLED) == SETTLED;
    }

    public boolean hasState() {
        return (modified & STATE) == STATE;
    }

    public boolean hasBatchable() {
        return (modified & BATCHABLE) == BATCHABLE;
    }

    //----- Access to the member data with state checks

    public Role getRole() {
        return role;
    }

    public Disposition setRole(Role role) {
        if (role == null) {
            throw new NullPointerException("Role cannot be null");
        } else {
            modified |= ROLE;
        }

        this.role = role;
        return this;
    }

    public long getFirst() {
        return first;
    }

    public Disposition setFirst(long first) {
        if (first < 0 || first > UINT_MAX) {
            throw new IllegalArgumentException("First value given is out of range: " + first);
        } else {
            modified |= FIRST;
        }

        this.first = first;
        return this;
    }

    public long getLast() {
        return last;
    }

    public Disposition setLast(long last) {
        if (last < 0 || last > UnsignedShort.MAX_VALUE.intValue()) {
            throw new IllegalArgumentException("Last value given is out of range: " + last);
        } else {
            modified |= LAST;
        }

        this.last = last;
        return this;
    }

    public boolean getSettled() {
        return settled;
    }

    public Disposition setSettled(boolean settled) {
        this.modified |= SETTLED;
        this.settled = settled;
        return this;
    }

    public DeliveryState getState() {
        return state;
    }

    public Disposition setState(DeliveryState state) {
        if (state != null) {
            this.modified |= STATE;
        } else {
            this.modified &= ~STATE;
        }

        this.state = state;
        return this;
    }

    public boolean getBatchable() {
        return batchable;
    }

    public Disposition setBatchable(boolean batchable) {
        this.modified |= BATCHABLE;
        this.batchable = batchable;
        return this;
    }

    @Override
    public Disposition copy() {
        Disposition copy = new Disposition();

        copy.role = role;
        copy.first = first;
        copy.last = last;
        copy.settled = settled;
        copy.state = state;
        copy.batchable = batchable;
        copy.modified = modified;

        return copy;
    }

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.DISPOSITION;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, ProtonBuffer payload, int channel, E context) {
        handler.handleDisposition(this, payload, channel, context);
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
