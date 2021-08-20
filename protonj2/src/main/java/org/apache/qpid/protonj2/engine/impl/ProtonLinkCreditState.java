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
package org.apache.qpid.protonj2.engine.impl;

import org.apache.qpid.protonj2.engine.LinkCreditState;
import org.apache.qpid.protonj2.types.transport.Flow;

/**
 * Holds the current credit state for a given link.
 */
public class ProtonLinkCreditState implements LinkCreditState {

    private int credit;
    private int deliveryCount;

    private boolean drain;
    private boolean echo;

    private boolean deliveryCountInitialized;

    @SuppressWarnings("unused")
    private long remoteDeliveryCount;
    @SuppressWarnings("unused")
    private long remoteLinkCredit;

    public ProtonLinkCreditState() {}

    public ProtonLinkCreditState(int deliveryCount) {
        initializeDeliveryCount(deliveryCount);
    }

    @Override
    public int getCredit() {
        return credit;
    }

    @Override
    public int getDeliveryCount() {
        return deliveryCount;
    }

    @Override
    public boolean isDrain() {
        return drain;
    }

    @Override
    public boolean isEcho() {
        return echo;
    }

    //----- Internal API for managing credit state

    boolean hasCredit() {
        return Integer.compareUnsigned(credit, 0) > 0;
    }

    void clearDrain() {
        drain = false;
    }

    void clearEcho() {
        echo = false;
    }

    void clearCredit() {
        credit = 0;
    }

    void incrementCredit(int credit) {
        this.credit += credit;
    }

    void decrementCredit() {
        credit = credit == 0 ? 0 : credit - 1;
    }

    int incrementDeliveryCount() {
        return deliveryCount++;
    }

    int incrementDeliveryCount(int amount) {
        return deliveryCount += amount;
    }

    int decrementDeliveryCount() {
        return deliveryCount--;
    }

    boolean isDeliveryCountInitialized() {
        return deliveryCountInitialized;
    }

    void initializeDeliveryCount(int deliveryCount) {
        this.deliveryCount = deliveryCount;
        deliveryCountInitialized = true;
    }

    public void updateCredit(int effectiveCredit) {
        // TODO: change credit to a long, or ensure increments/decrements above work fully if it has wrapped.
        this.credit = effectiveCredit;
    }

    public void updateDeliveryCount(int deliveryCount) {
        // TODO: change deliveryCount to a long, or fix uses to account for it being a wrapping int
        this.deliveryCount = deliveryCount;
    }

    void remoteFlow(Flow flow) {
        remoteDeliveryCount = flow.getDeliveryCount();
        remoteLinkCredit = flow.getLinkCredit();
        echo = flow.getEcho();
        drain = flow.getDrain();
    }

    /**
     * Creates a snapshot of the current credit state, a subclass should implement this
     * method and provide an appropriately populated snapshot of the current state.
     *
     * @return a snapshot of the current credit state.
     */
    LinkCreditState snapshot() {
        return new UnmodifiableLinkCreditState(credit, deliveryCount, drain, echo);
    }

    //----- Provide an immutable view type for protection

    private static class UnmodifiableLinkCreditState implements LinkCreditState {

        private final int credit;
        private final int deliveryCount;
        private final boolean drain;
        private final boolean echo;

        public UnmodifiableLinkCreditState(int credit, int deliveryCount, boolean drain, boolean echo) {
            this.credit = credit;
            this.deliveryCount = deliveryCount;
            this.drain = drain;
            this.echo = echo;
        }

        @Override
        public int getCredit() {
            return credit;
        }

        @Override
        public int getDeliveryCount() {
            return deliveryCount;
        }

        @Override
        public boolean isDrain() {
            return drain;
        }

        @Override
        public boolean isEcho() {
            return echo;
        }
    }
}
