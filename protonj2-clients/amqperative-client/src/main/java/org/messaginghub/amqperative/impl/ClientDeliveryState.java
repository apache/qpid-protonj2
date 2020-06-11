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
package org.messaginghub.amqperative.impl;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.messaging.Accepted;
import org.apache.qpid.proton4j.types.messaging.Modified;
import org.apache.qpid.proton4j.types.messaging.Rejected;
import org.apache.qpid.proton4j.types.messaging.Released;
import org.apache.qpid.proton4j.types.transactions.TransactionalState;
import org.apache.qpid.proton4j.types.transport.ErrorCondition;
import org.messaginghub.amqperative.DeliveryState;

/**
 * Client internal implementation of a DeliveryState type.
 */
public abstract class ClientDeliveryState implements DeliveryState {

    //----- Abstract methods for nested types

    /**
     * Returns the Proton version of the specific {@link org.apache.qpid.proton4j.amqp.transport.DeliveryState} that
     * this type represents.
     *
     * @return the Proton state object that this type maps to.
     */
    abstract org.apache.qpid.proton4j.types.transport.DeliveryState getProtonDeliveryState();

    //----- Create Delivery State from Proton instance

    static DeliveryState fromProtonType(org.apache.qpid.proton4j.types.messaging.Outcome outcome) {
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

    static DeliveryState fromProtonType(org.apache.qpid.proton4j.types.transport.DeliveryState state) {
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

    static org.apache.qpid.proton4j.types.transport.DeliveryState asProtonType(DeliveryState state) {
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
                    return new Rejected(); // TODO - How do we aggregate the different values into one DeliveryState Object
                case MODIFIED:
                    return new Modified(); // TODO - How do we aggregate the different values into one DeliveryState Object
                case TRANSACTIONAL:
                    throw new IllegalArgumentException("Cannot manually enlist delivery in AMQP Transactions");
                default:
                    throw new UnsupportedOperationException("Client does not support the given Delivery State type: " + state.getType());
            }
        }
    }

    //----- Delivery State implementations

    public static class ClientAccepted extends ClientDeliveryState {

        private static final ClientAccepted INSTANCE = new ClientAccepted();

        @Override
        public Type getType() {
            return Type.ACCEPTED;
        }

        @Override
        org.apache.qpid.proton4j.types.transport.DeliveryState getProtonDeliveryState() {
            return Accepted.getInstance();
        }

        public static ClientAccepted getInstance() {
            return INSTANCE;
        }
    }

    public static class ClientReleased extends ClientDeliveryState {

        private static final ClientReleased INSTANCE = new ClientReleased();

        @Override
        public Type getType() {
            return Type.RELEASED;
        }

        @Override
        org.apache.qpid.proton4j.types.transport.DeliveryState getProtonDeliveryState() {
            return Released.getInstance();
        }

        public static ClientReleased getInstance() {
            return INSTANCE;
        }
    }

    public static class ClientRejected extends ClientDeliveryState {

        private final Rejected rejected = new Rejected();

        ClientRejected(Rejected rejected) {
            if (rejected.getError() != null) {
                rejected.setError(rejected.getError().copy());
            }
        }

        public ClientRejected(String condition, String description) {
            if (condition != null || description != null) {
                rejected.setError(new ErrorCondition(Symbol.valueOf(condition), description));
            }
        }

        public ClientRejected(String condition, String description, Map<String, Object> info) {
            if (condition != null || description != null) {
                rejected.setError(new ErrorCondition(
                    Symbol.valueOf(condition), description, ClientConversionSupport.toSymbolKeyedMap(info)));
            }
        }

        @Override
        public Type getType() {
            return Type.RELEASED;
        }

        @Override
        org.apache.qpid.proton4j.types.transport.DeliveryState getProtonDeliveryState() {
            return rejected;
        }

        public static ClientRejected fromProtonType(Rejected rejected) {
            return new ClientRejected(rejected);
        }
    }

    public static class ClientModified extends ClientDeliveryState {

        private final Modified modified = new Modified();

        ClientModified(Modified modified) {
            this.modified.setDeliveryFailed(modified.isDeliveryFailed());
            this.modified.setUndeliverableHere(modified.isUndeliverableHere());
            this.modified.setMessageAnnotations(new LinkedHashMap<>(modified.getMessageAnnotations()));
        }

        public ClientModified(boolean failed, boolean undeliverable) {
            modified.setDeliveryFailed(failed);
            modified.setUndeliverableHere(undeliverable);
        }

        public ClientModified(boolean failed, boolean undeliverable, Map<String, Object> annotations) {
            modified.setDeliveryFailed(failed);
            modified.setUndeliverableHere(undeliverable);
            modified.setMessageAnnotations(ClientConversionSupport.toSymbolKeyedMap(annotations));
        }

        @Override
        public Type getType() {
            return Type.MODIFIED;
        }

        @Override
        org.apache.qpid.proton4j.types.transport.DeliveryState getProtonDeliveryState() {
            return modified;
        }

        public static ClientModified fromProtonType(Modified modified) {
            return new ClientModified(modified);
        }
    }

    public static class ClientTransactional extends ClientDeliveryState {

        private final TransactionalState txnState = new TransactionalState();

        ClientTransactional(TransactionalState txnState) {
            this.txnState.setOutcome(txnState.getOutcome());
            this.txnState.setTxnId(txnState.getTxnId().copy());
        }

        @Override
        public Type getType() {
            return Type.TRANSACTIONAL;
        }

        @Override
        org.apache.qpid.proton4j.types.transport.DeliveryState getProtonDeliveryState() {
            return txnState;
        }

        public static ClientTransactional fromProtonType(TransactionalState txnState) {
            return new ClientTransactional(txnState);
        }
    }
}
