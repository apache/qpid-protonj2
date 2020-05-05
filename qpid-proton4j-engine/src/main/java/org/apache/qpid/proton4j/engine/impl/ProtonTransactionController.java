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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.transactions.Coordinator;
import org.apache.qpid.proton4j.amqp.transactions.Declare;
import org.apache.qpid.proton4j.amqp.transactions.Discharge;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.Transaction;
import org.apache.qpid.proton4j.engine.TransactionController;
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;

/**
 * {@link TransactionController} implementation that implements the abstraction
 * around a sender link that initiates requests to {@link Declare} and to
 * {@link Discharge} AMQP {@link Transaction} instance.
 */
public class ProtonTransactionController extends ProtonEndpoint<TransactionController> implements TransactionController {

    private final ProtonSender senderLink;

    private final List<ProtonControllerTransaction> transactions = new ArrayList<>();

    public ProtonTransactionController(ProtonSender senderLink) {
        super(senderLink.getEngine());

        this.senderLink = senderLink;
    }

    @Override
    public ProtonSession getParent() {
        return senderLink.getSession();
    }

    @Override
    ProtonTransactionController self() {
        return this;
    }

    @Override
    public Transaction<TransactionController> declare() {
        if (!senderLink.isSendable()) {
            throw new IllegalStateException("Cannot Declare due to current capicity restrictions.");
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TransactionController discharge(Transaction<TransactionController> transaction, boolean failed) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionController declaredHandler(EventHandler<Transaction<TransactionController>> declaredEventHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionController declareFailureHandler(EventHandler<Transaction<TransactionController>> declareFailureEventHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionController dischargedHandler(EventHandler<Transaction<TransactionController>> dischargedEventHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionController dischargeFailureHandler(EventHandler<Transaction<TransactionController>> dischargeFailureEventHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    //----- Hand off methods for link specific elements.

    @Override
    public TransactionController open() throws IllegalStateException, EngineStateException {
        senderLink.open();
        return this;
    }

    @Override
    public TransactionController close() throws EngineFailedException {
        senderLink.close();
        return this;
    }

    @Override
    public boolean isLocallyOpen() {
        senderLink.isLocallyOpen();
        return false;
    }

    @Override
    public boolean isLocallyClosed() {
        senderLink.isLocallyClosed();
        return false;
    }

    @Override
    public TransactionController setSource(Source source) throws IllegalStateException {
        senderLink.setSource(source);
        return this;
    }

    @Override
    public Source getSource() {
        return senderLink.getSource();
    }

    @Override
    public TransactionController setCoordinator(Coordinator coordinator) throws IllegalStateException {
        senderLink.setTarget(coordinator);
        return this;
    }

    @Override
    public Coordinator getCoordinator() {
        return senderLink.getTarget();
    }

    @Override
    public ErrorCondition getCondition() {
        return senderLink.getCondition();
    }

    @Override
    public TransactionController setCondition(ErrorCondition condition) {
        senderLink.setCondition(condition);
        return this;
    }

    @Override
    public Map<Symbol, Object> getProperties() {
        return senderLink.getProperties();
    }

    @Override
    public TransactionController setProperties(Map<Symbol, Object> properties) throws IllegalStateException {
        senderLink.setProperties(properties);
        return this;
    }

    @Override
    public TransactionController setOfferedCapabilities(Symbol... offeredCapabilities) throws IllegalStateException {
        senderLink.setOfferedCapabilities(offeredCapabilities);
        return this;
    }

    @Override
    public Symbol[] getOfferedCapabilities() {
        return senderLink.getOfferedCapabilities();
    }

    @Override
    public TransactionController setDesiredCapabilities(Symbol... desiredCapabilities) throws IllegalStateException {
        senderLink.setDesiredCapabilities(desiredCapabilities);
        return this;
    }

    @Override
    public Symbol[] getDesiredCapabilities() {
        return senderLink.getDesiredCapabilities();
    }

    @Override
    public boolean isRemotelyOpen() {
        return senderLink.isRemotelyOpen();
    }

    @Override
    public boolean isRemotelyClosed() {
        return senderLink.isRemotelyClosed();
    }

    @Override
    public Symbol[] getRemoteOfferedCapabilities() {
        return senderLink.getRemoteOfferedCapabilities();
    }

    @Override
    public Symbol[] getRemoteDesiredCapabilities() {
        return senderLink.getRemoteDesiredCapabilities();
    }

    @Override
    public Map<Symbol, Object> getRemoteProperties() {
        return senderLink.getRemoteProperties();
    }

    @Override
    public ErrorCondition getRemoteCondition() {
        return senderLink.getRemoteCondition();
    }

    @Override
    public Source getRemoteSource() {
        return senderLink.getRemoteSource();
    }

    @Override
    public Coordinator getRemoteCoordinator() {
        return senderLink.getRemoteTarget();
    }

    //----- The Controller specific Transaction implementation

    private final class ProtonControllerTransaction extends ProtonTransaction<ProtonTransactionController> {

        private final ProtonTransactionController controller;

        public ProtonControllerTransaction(ProtonTransactionController controller) {
            this.controller = controller;
        }

        @Override
        public ProtonTransactionController parent() {
            return controller;
        }
    }
}
