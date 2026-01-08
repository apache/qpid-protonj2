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
package org.apache.qpid.protonj2.client.impl;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.Transactional;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.transactions.TransactionalState;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

/**
 * Client internal implementation of a DeliveryState type.
 */
public abstract class ClientDeliveryState implements DeliveryState {

    //----- Abstract methods for nested types

    /**
     * Returns the Proton version of the specific {@link org.apache.qpid.protonj2.client.DeliveryState} that
     * this type represents.
     *
     * @return the Proton state object that this type maps to.
     */
    abstract org.apache.qpid.protonj2.types.transport.DeliveryState getProtonDeliveryState();

    //----- Create Delivery State from Proton instance

    static DeliveryState fromProtonType(org.apache.qpid.protonj2.types.messaging.Outcome outcome) {
        if (outcome == null) {
            return null;
        }

        if (outcome instanceof Accepted) {
            return ClientAccepted.getInstance();
        } else if (outcome instanceof Released) {
            return ClientReleased.getInstance();
        } else if (outcome instanceof Rejected) {
            return ClientRejected.fromProtonType((Rejected) outcome);
        } else if (outcome instanceof Modified) {
            return ClientModified.fromProtonType((Modified) outcome);
        }

        throw new IllegalArgumentException("Cannot map to unknown Proton Outcome to a DeliveryStateType: " + outcome);
    }

    static DeliveryState fromProtonType(org.apache.qpid.protonj2.types.transport.DeliveryState state) {
        if (state == null) {
            return null;
        }

        switch (state.getType()) {
            case Accepted:
                return ClientAccepted.getInstance();
            case Released:
                return ClientReleased.getInstance();
            case Rejected:
                return ClientRejected.fromProtonType((Rejected) state);
            case Modified:
                return ClientModified.fromProtonType((Modified) state);
            case Transactional:
                return ClientTransactional.fromProtonType((TransactionalState) state);
            default:
                throw new IllegalArgumentException("Cannot map to unknown Proton Delivery State type");
        }
    }

    static DeliveryState.Type fromOutcomeSymbol(Symbol outcome) {
        if (outcome == null) {
            return null;
        }

        try {
            return DeliveryState.Type.valueOf(outcome.toString().toUpperCase());
        } catch (Throwable error) {
            throw new IllegalArgumentException("Cannot map outcome name to unknown Proton DeliveryState.Type");
        }
    }

    static org.apache.qpid.protonj2.types.transport.DeliveryState asProtonType(DeliveryState state) {
        if (state == null) {
            return null;
        } else if (state instanceof ClientDeliveryState) {
            return ((ClientDeliveryState) state).getProtonDeliveryState();
        } else {
            switch (state.getType()) {
                case ACCEPTED:
                    return Accepted.getInstance();
                case RELEASED:
                    return Released.getInstance();
                case REJECTED:
                    return ClientRejected.fromUnknownClientType(state);
                case MODIFIED:
                    return ClientModified.fromUnknownClientType(state);
                case TRANSACTIONAL:
                    throw new IllegalArgumentException("Cannot manually enlist delivery in AMQP Transactions");
                default:
                    throw new UnsupportedOperationException("Client does not support the given Delivery State type: " + state.getType());
            }
        }
    }

    //----- Delivery State implementations

    /**
     * Client defined {@link Accepted} delivery state definition
     */
    public static class ClientAccepted extends ClientDeliveryState implements org.apache.qpid.protonj2.client.Accepted {

        private static final ClientAccepted INSTANCE = new ClientAccepted();

        private ClientAccepted() {
            // Singleton
        }

        @Override
        org.apache.qpid.protonj2.types.transport.DeliveryState getProtonDeliveryState() {
            return Accepted.getInstance();
        }

        /**
         * @return a singleton instance of the accepted type.
         */
        public static ClientAccepted getInstance() {
            return INSTANCE;
        }
    }

    /**
     * Client defined {@link Released} delivery state definition
     */
    public static class ClientReleased extends ClientDeliveryState implements org.apache.qpid.protonj2.client.Released {

        private static final ClientReleased INSTANCE = new ClientReleased();

        private ClientReleased() {
            // Singleton
        }

        @Override
        org.apache.qpid.protonj2.types.transport.DeliveryState getProtonDeliveryState() {
            return Released.getInstance();
        }

        /**
         * @return a singleton instance of the released type.
         */
        public static ClientReleased getInstance() {
            return INSTANCE;
        }
    }

    /**
     * Client defined {@link Rejected} delivery state definition
     */
    public static class ClientRejected extends ClientDeliveryState implements org.apache.qpid.protonj2.client.Rejected {

        private final Rejected rejected = new Rejected();

        ClientRejected(Rejected source) {
            if (source.getError() != null) {
                rejected.setError(source.getError().copy());
            }
        }

        /**
         * Create a new Rejected outcome with the given description of the rejection cause.
         *
         * @param condition
         * 		the condition that defines the rejection outcome.
         * @param description
         *      the description of the underlying cause of the rejection.
         */
        public ClientRejected(String condition, String description) {
            if (condition != null || description != null) {
                rejected.setError(new ErrorCondition(Symbol.valueOf(condition), description));
            }
        }

        /**
         * Create a new Rejected outcome with the given description of the rejection cause.
         *
         * @param condition
         * 		the condition that defines the rejection outcome.
         * @param description
         *      the description of the underlying cause of the rejection.
         * @param info
         * 		additional information to be provide to the remote about the rejection.
         */
        public ClientRejected(String condition, String description, Map<String, Object> info) {
            if (condition != null || description != null) {
                rejected.setError(new ErrorCondition(
                    Symbol.valueOf(condition), description, ClientConversionSupport.toSymbolKeyedMap(info)));
            }
        }

        @Override
        org.apache.qpid.protonj2.types.transport.DeliveryState getProtonDeliveryState() {
            return rejected;
        }

        @Override
        public String getCondition() {
            if (rejected != null && rejected.getError() != null && rejected.getError().getCondition() != null) {
                return rejected.getError().getCondition().toString();
            } else {
                return null;
            }
        }

        @Override
        public String getDescription() {
            if (rejected != null && rejected.getError() != null) {
                return rejected.getError().getDescription();
            } else {
                return null;
            }
        }

        @Override
        public Map<String, Object> getInfo() {
            if (rejected != null && rejected.getError() != null) {
                return ClientConversionSupport.toStringKeyedMap(rejected.getError().getInfo());
            } else {
                return null;
            }
        }

        static ClientRejected fromProtonType(Rejected rejected) {
            return new ClientRejected(rejected);
        }

        static Rejected fromUnknownClientType(DeliveryState deliveryState) {
            if (deliveryState instanceof org.apache.qpid.protonj2.client.Rejected) {
                org.apache.qpid.protonj2.client.Rejected rejectedState = (org.apache.qpid.protonj2.client.Rejected) deliveryState;

                return new Rejected(new ErrorCondition(rejectedState.getCondition(),
                                                       rejectedState.getDescription(),
                                                       ClientConversionSupport.toSymbolKeyedMap(rejectedState.getInfo())));
            } else {
                return new Rejected(); // TODO: This loses data from the source but remains backwards compatible
            }
        }
    }

    /**
     * Client defined {@link Modified} delivery state definition
     */
    public static class ClientModified extends ClientDeliveryState implements org.apache.qpid.protonj2.client.Modified {

        private final Modified modified = new Modified();

        ClientModified(Modified modified) {
            this.modified.setDeliveryFailed(modified.isDeliveryFailed());
            this.modified.setUndeliverableHere(modified.isUndeliverableHere());
            if (modified.getMessageAnnotations() != null) {
                this.modified.setMessageAnnotations(new LinkedHashMap<>(modified.getMessageAnnotations()));
            }
        }

        /**
         * Create a new instance with the given outcome values.
         *
         * @param failed
         * 		has the delivery failed
         * @param undeliverable
         *      is the delivery no longer deliverable to this client.
         */
        public ClientModified(boolean failed, boolean undeliverable) {
            modified.setDeliveryFailed(failed);
            modified.setUndeliverableHere(undeliverable);
        }

        /**
         * Create a new instance with the given outcome values.
         *
         * @param failed
         * 		has the delivery failed
         * @param undeliverable
         *      is the delivery no longer deliverable to this client.
         * @param annotations
         *      updated annotation values to provide to the remote.
         */
        public ClientModified(boolean failed, boolean undeliverable, Map<String, Object> annotations) {
            modified.setDeliveryFailed(failed);
            modified.setUndeliverableHere(undeliverable);
            modified.setMessageAnnotations(ClientConversionSupport.toSymbolKeyedMap(annotations));
        }

        @Override
        org.apache.qpid.protonj2.types.transport.DeliveryState getProtonDeliveryState() {
            return modified;
        }

        @Override
        public boolean isDeliveryFailed() {
            return modified.isDeliveryFailed();
        }

        @Override
        public boolean isUndeliverableHere() {
            return modified.isUndeliverableHere();
        }

        @Override
        public Map<String, Object> getMessageAnnotations() {
            return ClientConversionSupport.toStringKeyedMap(modified.getMessageAnnotations());
        }

        static ClientModified fromProtonType(Modified modified) {
            return new ClientModified(modified);
        }

        static Modified fromUnknownClientType(DeliveryState deliveryState) {
            if (deliveryState instanceof org.apache.qpid.protonj2.client.Modified) {
                org.apache.qpid.protonj2.client.Modified modifiedState = (org.apache.qpid.protonj2.client.Modified) deliveryState;

                return new Modified(modifiedState.isDeliveryFailed(), modifiedState.isUndeliverableHere(),
                                    ClientConversionSupport.toSymbolKeyedMap(modifiedState.getMessageAnnotations()));
            } else {
                return new Modified(); // TODO: This loses data from the source but remains backwards compatible
            }
        }

    }

    /**
     * Client defined {@link TransactionalState} delivery state definition
     */
    public static class ClientTransactional extends ClientDeliveryState implements Transactional {

        private final TransactionalState txnState = new TransactionalState();

        ClientTransactional(TransactionalState txnState) {
            this.txnState.setOutcome(txnState.getOutcome());
            this.txnState.setTxnId(txnState.getTxnId().copy());
        }

        @Override
        public boolean isAccepted() {
            return txnState.getOutcome() instanceof Accepted;
        }

        @Override
        public boolean isRejected() {
            return txnState.getOutcome() instanceof Rejected;
        }

        @Override
        public boolean isReleased() {
            return txnState.getOutcome() instanceof Released;
        }

        @Override
        public boolean isModified() {
            return txnState.getOutcome() instanceof Modified;
        }

        @Override
        public DeliveryState getOutcome() {
            return fromProtonType(txnState.getOutcome());
        }

        @Override
        org.apache.qpid.protonj2.types.transport.DeliveryState getProtonDeliveryState() {
            return txnState;
        }

        static ClientTransactional fromProtonType(TransactionalState txnState) {
            return new ClientTransactional(txnState);
        }
    }
}
