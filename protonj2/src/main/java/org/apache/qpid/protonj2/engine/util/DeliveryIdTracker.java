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
package org.apache.qpid.protonj2.engine.util;

import org.apache.qpid.protonj2.types.UnsignedInteger;

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

    /**
     * @return true if the tracker is not currently tracking a delivery Id.
     */
    public boolean isEmpty() {
        return empty;
    }

    /**
     * Compares the tracked delivery id value to the {@link Number} given, if no value is being
     * tracked this method returns -1.  This method returns 0 if the tracked id is equal to the
     * value provided, value less than 0 if the tracked id is less than the provided value; and a
     * value greater than 0 if the tracked id is larger than the value provided.
     *
     * @param other
     * 		The {@link Number} to compare the tracked id with.
     *
     * @return the result of comparing the tracked id to the provided number.
     */
    public int compareTo(Number other) {
        if (isEmpty()) {
            return -1;
        } else {
            return Integer.compareUnsigned(intValue(), other.intValue());
        }
    }

    /**
     * Compares the tracked delivery id value to the <code>int</code> given, if no value is being
     * tracked this method returns -1.  This method returns 0 if the tracked id is equal to the
     * value provided, value less than 0 if the tracked id is less than the provided value; and a
     * value greater than 0 if the tracked id is larger than the value provided.
     *
     * @param other
     * 		The primitive {@link Integer} to compare the tracked id with.
     *
     * @return the result of comparing the tracked id to the provided primitive integer value.
     */
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
        return Float.intBitsToFloat(deliveryId);
    }

    @Override
    public double doubleValue() {
        return Float.floatToIntBits(deliveryId);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof DeliveryIdTracker) {
            return ((DeliveryIdTracker) other).deliveryId == this.deliveryId;
        } else if (other instanceof SequenceNumber) {
            return ((SequenceNumber) other).intValue() == this.deliveryId;
        } else if (other instanceof UnsignedInteger) {
            return ((UnsignedInteger) other).intValue() == this.deliveryId;
        }

        return false;
    }

    /**
     * Performs an unsigned comparison between the value being tracked and the integer value
     * passed, if no id is currently being tracked then this method returns false.
     *
     * @param other
     * 		The value to compare to the currently tracked id.
     *
     * @return true if the tracked delivery id matches the integer value provided.
     */
    public boolean equals(int other) {
        return Integer.compareUnsigned(deliveryId, other) == 0;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(deliveryId);
    }

    /**
     * @return an {@link UnsignedInteger} view of the tracked delivery id, or null if not tracking at this time.
     */
    public UnsignedInteger toUnsignedInteger() {
        return empty ? null : UnsignedInteger.valueOf(deliveryId);
    }

    @Override
    public String toString() {
        return Integer.toUnsignedString(deliveryId);
    }
}
