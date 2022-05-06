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

import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.types.transport.DeliveryState;

/**
 * A pass-through {@link ClientTransactionContext} that is used when a session has not had any active
 * transactions.
 */
final class ClientNoOpTransactionContext implements ClientTransactionContext {

    @Override
    public ClientTransactionContext begin(ClientFuture<Session> beginFuture) throws ClientIllegalStateException {
        throw new ClientIllegalStateException("Cannot begin from a no-op transaction context");
    }

    @Override
    public ClientTransactionContext commit(ClientFuture<Session> commitFuture, boolean startNew) throws ClientIllegalStateException {
        throw new ClientIllegalStateException("Cannot commit from a no-op transaction context");
    }

    @Override
    public ClientTransactionContext rollback(ClientFuture<Session> rollbackFuture, boolean startNew) throws ClientIllegalStateException {
        throw new ClientIllegalStateException("Cannot roll back from a no-op transaction context");
    }

    @Override
    public boolean isInTransaction() {
        return false;
    }

    @Override
    public boolean isRollbackOnly() {
        return false;
    }

    @Override
    public ClientTransactionContext send(Sendable sendable, DeliveryState outcome, boolean settled) {
        sendable.send(outcome, settled);
        return this;
    }

    @Override
    public ClientTransactionContext disposition(IncomingDelivery delivery, DeliveryState outcome, boolean settled) {
        delivery.disposition(outcome, settled);
        return this;
    }
}
