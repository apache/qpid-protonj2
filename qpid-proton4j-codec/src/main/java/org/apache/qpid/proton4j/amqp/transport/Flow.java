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

import java.util.Map;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;

public final class Flow implements Performative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000013L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:flow:list");

    private UnsignedInteger nextIncomingId;
    private UnsignedInteger incomingWindow;
    private UnsignedInteger nextOutgoingId;
    private UnsignedInteger outgoingWindow;
    private UnsignedInteger handle;
    private UnsignedInteger deliveryCount;
    private UnsignedInteger linkCredit;
    private UnsignedInteger available;
    private boolean drain;
    private boolean echo;
    private Map<Object, Object> properties;

    public UnsignedInteger getNextIncomingId() {
        return nextIncomingId;
    }

    public void setNextIncomingId(UnsignedInteger nextIncomingId) {
        this.nextIncomingId = nextIncomingId;
    }

    public UnsignedInteger getIncomingWindow() {
        return incomingWindow;
    }

    public void setIncomingWindow(UnsignedInteger incomingWindow) {
        if (incomingWindow == null) {
            throw new NullPointerException("the incoming-window field is mandatory");
        }

        this.incomingWindow = incomingWindow;
    }

    public UnsignedInteger getNextOutgoingId() {
        return nextOutgoingId;
    }

    public void setNextOutgoingId(UnsignedInteger nextOutgoingId) {
        if (nextOutgoingId == null) {
            throw new NullPointerException("the next-outgoing-id field is mandatory");
        }

        this.nextOutgoingId = nextOutgoingId;
    }

    public UnsignedInteger getOutgoingWindow() {
        return outgoingWindow;
    }

    public void setOutgoingWindow(UnsignedInteger outgoingWindow) {
        if (outgoingWindow == null) {
            throw new NullPointerException("the outgoing-window field is mandatory");
        }

        this.outgoingWindow = outgoingWindow;
    }

    public UnsignedInteger getHandle() {
        return handle;
    }

    public void setHandle(UnsignedInteger handle) {
        this.handle = handle;
    }

    public UnsignedInteger getDeliveryCount() {
        return deliveryCount;
    }

    public void setDeliveryCount(UnsignedInteger deliveryCount) {
        this.deliveryCount = deliveryCount;
    }

    public UnsignedInteger getLinkCredit() {
        return linkCredit;
    }

    public void setLinkCredit(UnsignedInteger linkCredit) {
        this.linkCredit = linkCredit;
    }

    public UnsignedInteger getAvailable() {
        return available;
    }

    public void setAvailable(UnsignedInteger available) {
        this.available = available;
    }

    public boolean getDrain() {
        return drain;
    }

    public void setDrain(boolean drain) {
        this.drain = drain;
    }

    public boolean getEcho() {
        return echo;
    }

    public void setEcho(boolean echo) {
        this.echo = echo;
    }

    public Map<Object, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<Object, Object> properties) {
        this.properties = properties;
    }

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.Flow;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, Binary payload, E context) {
        handler.handleFlow(this, payload, context);
    }

    @Override
    public String toString() {
        return "Flow{" +
               "nextIncomingId=" + nextIncomingId +
               ", incomingWindow=" + incomingWindow +
               ", nextOutgoingId=" + nextOutgoingId +
               ", outgoingWindow=" + outgoingWindow +
               ", handle=" + handle +
               ", deliveryCount=" + deliveryCount +
               ", linkCredit=" + linkCredit +
               ", available=" + available +
               ", drain=" + drain +
               ", echo=" + echo +
               ", properties=" + properties +
               '}';
    }
}
