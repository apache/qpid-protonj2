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
package org.apache.qpid.proton4j.amqp.messaging;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedByte;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;

public final class Header implements Section {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000070L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:header:list");

    private Boolean durable;
    private UnsignedByte priority;
    private UnsignedInteger ttl;
    private Boolean firstAcquirer;
    private UnsignedInteger deliveryCount;

    public Header() {
    }

    public Header(Header other) {
        this.durable = other.durable;
        this.priority = other.priority;
        this.ttl = other.ttl;
        this.firstAcquirer = other.firstAcquirer;
        this.deliveryCount = other.deliveryCount;
    }

    public Boolean getDurable() {
        return durable;
    }

    public void setDurable(Boolean durable) {
        this.durable = durable;
    }

    public UnsignedByte getPriority() {
        return priority;
    }

    public void setPriority(UnsignedByte priority) {
        this.priority = priority;
    }

    public UnsignedInteger getTtl() {
        return ttl;
    }

    public void setTtl(UnsignedInteger ttl) {
        this.ttl = ttl;
    }

    public Boolean getFirstAcquirer() {
        return firstAcquirer;
    }

    public void setFirstAcquirer(Boolean firstAcquirer) {
        this.firstAcquirer = firstAcquirer;
    }

    public UnsignedInteger getDeliveryCount() {
        return deliveryCount;
    }

    public void setDeliveryCount(UnsignedInteger deliveryCount) {
        this.deliveryCount = deliveryCount;
    }

    @Override
    public String toString() {
        return "Header{ " +
                "durable=" + durable +
                ", priority=" + priority +
                ", ttl=" + ttl +
                ", firstAcquirer=" + firstAcquirer +
                ", deliveryCount=" + deliveryCount + " }";
    }
}
