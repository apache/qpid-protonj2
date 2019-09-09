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
package org.apache.qpid.proton4j.engine.impl;

import org.apache.qpid.proton4j.engine.LinkCreditState;

/**
 * Holds the current credit state for a given link.
 */
public class ProtonLinkCreditState implements LinkCreditState {

    private int credit;
    private int deliveryCount;

    @Override
    public int getCredit() {
        return credit;
    }

    @Override
    public int getDeliveryCount() {
        return deliveryCount;
    }

    //----- Internal API for managing credit state

    ProtonLinkCreditState setCredit(int credit) {
        this.credit = credit;
        return this;
    }

    void decrementCredit() {
        credit = credit == 0 ? 0 : credit - 1;
    }

    ProtonLinkCreditState setDeliveryCount(int deliveryCount) {
        this.deliveryCount = deliveryCount;
        return this;
    }

    int incrementDeliveryCount() {
        return deliveryCount++;
    }

    int decrementDeliveryCount() {
        return deliveryCount--;
    }

    /**
     * Creates a snapshot of the current credit state, a subclass should implement this
     * method and provide an appropriately populated snapshot of the current state.
     *
     * @return a snapshot of the current credit state.
     */
    LinkCreditState snapshot() {
        return new UnmodifiableLinkCreditState(credit, deliveryCount);
    }

    //----- Provide an immutable view type for protection

    private static class UnmodifiableLinkCreditState implements LinkCreditState {

        private final int credit;
        private final int deliveryCount;

        public UnmodifiableLinkCreditState(int credit, int deliveryCount) {
            this.credit = credit;
            this.deliveryCount = deliveryCount;
        }

        @Override
        public int getCredit() {
            return credit;
        }

        @Override
        public int getDeliveryCount() {
            return deliveryCount;
        }
    }
}
