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
package org.apache.qpid.proton4j.engine.util;

import org.apache.qpid.proton4j.types.UnsignedInteger;

/**
 * Tracker of Delivery ID values, implements a sequence number and provides ability to
 * keep an not set state for use when allowing for set / not set tracking.
 */
public class DeliveryIdTracker extends Number implements Comparable<DeliveryIdTracker>  {

    private static final long serialVersionUID = -334270502498254343L;

    private int deliveryId;
    private boolean empty = true;

    /**
     * Create a new Delivery Id tracker with initial state.
     */
    public DeliveryIdTracker() {}

    /**
     * Create a new Delivery Id tracker with initial state.
     *
     * @param startValue
     *      The initial value to assign this tracker.
     */
    public DeliveryIdTracker(int startValue) {
        deliveryId = startValue;
        empty = false;
    }

    /**
     * Sets the current delivery ID value for this {@link DeliveryIdTracker}
     *
     * @param value
     *      The new value to assign as the delivery ID.
     */
    public void set(int value) {
        deliveryId = value;
        empty = false;
    }

    /**
     * Clears the tracked value and marks this tracker as empty.
     */
    public void reset() {
        deliveryId = 0;
        empty = true;
    }

    public boolean isEmpty() {
        return empty;
    }

    public int compareTo(SequenceNumber other) {
        if (isEmpty()) {
            return -1;
        } else {
            return Integer.compareUnsigned(intValue(), other.intValue());
        }
    }

    public int compareTo(int other) {
        if (isEmpty()) {
            return -1;
        } else {
            return Integer.compareUnsigned(intValue(), other);
        }
    }

    @Override
    public int compareTo(DeliveryIdTracker other) {
        if (isEmpty() || other.isEmpty()) {
            return -1;
        } else {
            return Integer.compareUnsigned(intValue(), other.deliveryId);
        }
    }

    @Override
    public int intValue() {
        return deliveryId;
    }

    @Override
    public long longValue() {
        return Integer.toUnsignedLong(deliveryId);
    }

    @Override
    public float floatValue() {
        return longValue();
    }

    @Override
    public double doubleValue() {
        return longValue();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof DeliveryIdTracker) {
            return ((DeliveryIdTracker) other).deliveryId == this.deliveryId;
        }

        return false;
    }

    public boolean equals(int other) {
        return Integer.compareUnsigned(deliveryId, other) == 0;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(deliveryId);
    }

    public UnsignedInteger toUnsignedInteger() {
        return empty ? null : UnsignedInteger.valueOf(deliveryId);
    }

    @Override
    public String toString() {
        return Integer.toUnsignedString(deliveryId);
    }
}
