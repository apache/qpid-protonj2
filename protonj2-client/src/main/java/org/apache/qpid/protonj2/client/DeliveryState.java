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
package org.apache.qpid.protonj2.client;

import java.util.Map;

import org.apache.qpid.protonj2.client.impl.ClientDeliveryState.ClientAccepted;
import org.apache.qpid.protonj2.client.impl.ClientDeliveryState.ClientModified;
import org.apache.qpid.protonj2.client.impl.ClientDeliveryState.ClientRejected;
import org.apache.qpid.protonj2.client.impl.ClientDeliveryState.ClientReleased;

/**
 * Conveys the outcome of a Delivery either incoming or outgoing.
 */
public interface DeliveryState {

    public enum Type {
        ACCEPTED,
        REJECTED,
        MODIFIED,
        RELEASED,
        TRANSACTIONAL
    }

    Type getType();

    /**
     * @return true if the {@link DeliveryState} represents an Accepted outcome.
     */
    default boolean isAccepted() {
        return getType() == Type.ACCEPTED;
    }

    //----- Factory methods for default DeliveryState types

    static DeliveryState accepted() {
        return ClientAccepted.getInstance();
    }

    static DeliveryState released() {
        return ClientReleased.getInstance();
    }

    static DeliveryState rejected(String condition, String description) {
        return new ClientRejected(condition, description);
    }

    static DeliveryState rejected(String condition, String description, Map<String, Object> info) {
        return new ClientRejected(condition, description, info);
    }

    static DeliveryState modified(boolean failed, boolean undeliverable) {
        return new ClientModified(failed, undeliverable);
    }

    static DeliveryState modified(boolean failed, boolean undeliverable, Map<String, Object> annotations) {
        return new ClientModified(failed, undeliverable, annotations);
    }
}
