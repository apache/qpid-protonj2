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

import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;

public final class Modified implements DeliveryState, Outcome {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000027L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:modified:list");

    private boolean deliveryFailed;
    private boolean undeliverableHere;
    private Map<Symbol, Object> messageAnnotations;

    public boolean getDeliveryFailed() {
        return deliveryFailed;
    }

    public void setDeliveryFailed(boolean deliveryFailed) {
        this.deliveryFailed = deliveryFailed;
    }

    public boolean getUndeliverableHere() {
        return undeliverableHere;
    }

    public void setUndeliverableHere(boolean undeliverableHere) {
        this.undeliverableHere = undeliverableHere;
    }

    public Map<Symbol, Object> getMessageAnnotations() {
        return messageAnnotations;
    }

    public void setMessageAnnotations(Map<Symbol, Object> messageAnnotations) {
        this.messageAnnotations = messageAnnotations;
    }

    @Override
    public String toString() {
        return "Modified{" +
               "deliveryFailed=" + deliveryFailed +
               ", undeliverableHere=" + undeliverableHere +
               ", messageAnnotations=" + messageAnnotations +
               '}';
    }
}
