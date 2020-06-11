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
package org.apache.qpid.proton4j.types.messaging;

import java.util.Map;

import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedLong;
import org.apache.qpid.proton4j.types.transport.DeliveryState;

public final class Modified implements DeliveryState, Outcome {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000027L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:modified:list");

    private boolean deliveryFailed;
    private boolean undeliverableHere;
    private Map<Symbol, Object> messageAnnotations;

    public boolean isDeliveryFailed() {
        return deliveryFailed;
    }

    public Modified setDeliveryFailed(boolean deliveryFailed) {
        this.deliveryFailed = deliveryFailed;
        return this;
    }

    public boolean isUndeliverableHere() {
        return undeliverableHere;
    }

    public Modified setUndeliverableHere(boolean undeliverableHere) {
        this.undeliverableHere = undeliverableHere;
        return this;
    }

    public Map<Symbol, Object> getMessageAnnotations() {
        return messageAnnotations;
    }

    @SuppressWarnings("unchecked")
    public Modified setMessageAnnotations(Map<Symbol, ?> messageAnnotations) {
        this.messageAnnotations = (Map<Symbol, Object>) messageAnnotations;
        return this;
    }

    @Override
    public String toString() {
        return "Modified{" +
               "deliveryFailed=" + deliveryFailed +
               ", undeliverableHere=" + undeliverableHere +
               ", messageAnnotations=" + messageAnnotations +
               '}';
    }

    @Override
    public DeliveryStateType getType() {
        return DeliveryStateType.Modified;
    }
}
