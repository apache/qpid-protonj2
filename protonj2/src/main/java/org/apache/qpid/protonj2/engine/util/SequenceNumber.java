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

/**
 * A mutable sequence that represents an unsigned integer type underneath
 */
public class SequenceNumber extends Number implements Comparable<SequenceNumber> {

    private static final long serialVersionUID = -1337181254740481576L;

    private int sequence;

    /**
     * Create a new sequence starting at the given value.
     *
     * @param startValue
     *      The starting value of this unsigned integer sequence
     */
    public SequenceNumber(int startValue) {
        this.sequence = startValue;
    }

    /**
     * Add one to the sequence value.
     *
     * @return this sequence.
     */
    public SequenceNumber increment() {
        sequence++;
        return this;
    }

    /**
     * Subtract one to the sequence value.
     *
     * @return this sequence.
     */
    public SequenceNumber decrement() {
        sequence--;
        return this;
    }

    /**
     * Add one to the sequence value.
     *
     * @return this sequence value prior to the increment.
     */
    public SequenceNumber getAndIncrement() {
        return new SequenceNumber(sequence++);
    }

    /**
     * Subtract one to the sequence value.
     *
     * @return this sequence value prior to the decrement.
     */
    public SequenceNumber getAndDecrement() {
        return new SequenceNumber(sequence--);
    }

    @Override
    public int intValue() {
        return sequence;
    }

    @Override
    public long longValue() {
        return Integer.toUnsignedLong(sequence);
    }

    @Override
    public float floatValue() {
        return Float.intBitsToFloat(sequence);
    }

    @Override
    public double doubleValue() {
        return Double.longBitsToDouble(longValue());
    }

    @Override
    public int compareTo(SequenceNumber other) {
        return Integer.compareUnsigned(sequence, other.sequence);
    }

    /**
     * Compares the sequence number value to the {@link Number} given.  This method returns 0 if
     * the tracked id is equal to the value provided, value less than 0 if the tracked id is less
     * than the provided value; and a value greater than 0 if the tracked id is larger than the value
     * provided.
     *
     * @param other
     * 		The {@link Number} to compare the current sequence number with.
     *
     * @return the result of comparing the current sequence number to the provided number.
     */
    public int compareTo(Number other) {
        return Integer.compareUnsigned(sequence, other.intValue());
    }

    /**
     * Compares the sequence number value to the primitive {@link Integer} given.  This method returns
     * 0 if the tracked id is equal to the value provided, value less than 0 if the tracked id is less
     * than the provided value; and a value greater than 0 if the tracked id is larger than the value
     * provided.
     *
     * @param other
     * 		The {@link Number} to compare the current sequence number with.
     *
     * @return the result of comparing the current sequence number to the provided number.
     */
    public int compareTo(int other) {
        return Integer.compareUnsigned(sequence, other);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof SequenceNumber) {
            return ((SequenceNumber) other).sequence == this.sequence;
        }

        return false;
    }

    /**
     * Performs an unsigned comparison between the current sequence number and the integer value
     * passed.
     *
     * @param other
     * 		The value to compare to the currently sequence number.
     *
     * @return true if the currently sequence value matches the integer value provided.
     */
    public boolean equals(int other) {
        return Integer.compareUnsigned(sequence, other) == 0;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(sequence);
    }

    @Override
    public String toString() {
        return Integer.toUnsignedString(sequence);
    }
}
