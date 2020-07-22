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
package org.apache.qpid.protonj2.types.messaging;

import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;

public final class Header implements Section<Header> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000070L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:header:list");

    public static final boolean DEFAULT_DURABILITY = false;
    public static final byte DEFAULT_PRIORITY = 4;
    public static final long DEFAULT_TIME_TO_LIVE = UnsignedInteger.MAX_VALUE.longValue();
    public static final boolean DEFAULT_FIRST_ACQUIRER = false;
    public static final long DEFAULT_DELIVERY_COUNT = 0;

    private static final long UINT_MAX = 0xFFFFFFFFL;

    private static int DURABLE = 1;
    private static int PRIORITY = 2;
    private static int TIME_TO_LIVE = 4;
    private static int FIRST_ACQUIRER = 8;
    private static int DELIVERY_COUNT = 16;

    private int modified = 0;

    private boolean durable = DEFAULT_DURABILITY;
    private byte priority = DEFAULT_PRIORITY;
    private long timeToLive = DEFAULT_TIME_TO_LIVE;
    private boolean firstAcquirer = DEFAULT_FIRST_ACQUIRER;
    private long deliveryCount = DEFAULT_DELIVERY_COUNT;

    public Header() {
    }

    public Header(Header other) {
        this.durable = other.durable;
        this.priority = other.priority;
        this.timeToLive = other.timeToLive;
        this.firstAcquirer = other.firstAcquirer;
        this.deliveryCount = other.deliveryCount;
    }

    public Header copy() {
        return new Header(this);
    }

    @Override
    public Header getValue() {
        return this;
    }

    //----- Query the state of the Header object -----------------------------//

    public boolean isEmpty() {
        return modified == 0;
    }

    public int getElementCount() {
        return 32 - Integer.numberOfLeadingZeros(modified);
    }

    public boolean hasDurable() {
        return (modified & DURABLE) == DURABLE;
    }

    public boolean hasPriority() {
        return (modified & PRIORITY) == PRIORITY;
    }

    public boolean hasTimeToLive() {
        return (modified & TIME_TO_LIVE) == TIME_TO_LIVE;
    }

    public boolean hasFirstAcquirer() {
        return (modified & FIRST_ACQUIRER) == FIRST_ACQUIRER;
    }

    public boolean hasDeliveryCount() {
        return (modified & DELIVERY_COUNT) == DELIVERY_COUNT;
    }

    //----- Access the AMQP Header object ------------------------------------//

    public boolean isDurable() {
        return durable;
    }

    public Header setDurable(boolean value) {
        if (value) {
            modified |= DURABLE;
        } else {
            modified &= ~DURABLE;
        }

        durable = value;
        return this;
    }

    public Header clearDurable() {
        modified &= ~DURABLE;
        durable = DEFAULT_DURABILITY;
        return this;
    }

    public byte getPriority() {
        return priority;
    }

    public Header setPriority(byte value) {
        if (value == DEFAULT_PRIORITY) {
            modified &= ~PRIORITY;
        } else {
            modified |= PRIORITY;
        }

        priority = value;
        return this;
    }

    public Header clearPriority() {
        modified &= ~PRIORITY;
        priority = DEFAULT_PRIORITY;
        return this;
    }

    public long getTimeToLive() {
        return timeToLive;
    }

    public Header setTimeToLive(long value) {
        if (value < 0 || value > UINT_MAX) {
            throw new IllegalArgumentException("TTL value given is out of range: " + value);
        } else {
            modified |= TIME_TO_LIVE;
        }

        timeToLive = value;
        return this;
    }

    public Header clearTimeToLive() {
        modified &= ~TIME_TO_LIVE;
        timeToLive = DEFAULT_TIME_TO_LIVE;
        return this;
    }

    public boolean isFirstAcquirer() {
        return firstAcquirer;
    }

    public Header setFirstAcquirer(boolean value) {
        if (value) {
            modified |= FIRST_ACQUIRER;
        } else {
            modified &= ~FIRST_ACQUIRER;
        }

        firstAcquirer = value;
        return this;
    }

    public Header clearFirstAcquirer() {
        modified &= ~FIRST_ACQUIRER;
        firstAcquirer = DEFAULT_FIRST_ACQUIRER;
        return this;
    }

    public long getDeliveryCount() {
        return deliveryCount;
    }

    public Header setDeliveryCount(long value) {
        if (value < 0 || value > UINT_MAX) {
            throw new IllegalArgumentException("Delivery Count value given is out of range: " + value);
        } else if (value == 0) {
            modified &= ~DELIVERY_COUNT;
        } else {
            modified |= DELIVERY_COUNT;
        }

        deliveryCount = value;
        return this;
    }

    public Header clearDeliveryCount() {
        modified &= ~DELIVERY_COUNT;
        deliveryCount = DEFAULT_DELIVERY_COUNT;
        return this;
    }

    public Header reset() {
        modified = 0;
        durable = DEFAULT_DURABILITY;
        priority = DEFAULT_PRIORITY;
        timeToLive = DEFAULT_TIME_TO_LIVE;
        firstAcquirer = DEFAULT_FIRST_ACQUIRER;
        deliveryCount = DEFAULT_DELIVERY_COUNT;
        return this;
    }

    @Override
    public String toString() {
        return "Header{ " +
                "durable=" + durable +
                ", priority=" + priority +
                ", ttl=" + timeToLive +
                ", firstAcquirer=" + firstAcquirer +
                ", deliveryCount=" + deliveryCount + " }";
    }

    @Override
    public SectionType getType() {
        return SectionType.Header;
    }
}
