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

import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
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
    public Transaction<TransactionController> declare() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TransactionController discharge(Transaction<TransactionController> transaction, boolean failed) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionController declared(EventHandler<Transaction<TransactionController>> declaredEventHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionController declareFailure(EventHandler<Transaction<TransactionController>> declareFailureEventHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionController discharged(EventHandler<Transaction<TransactionController>> dischargedEventHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionController dischargeFailure(EventHandler<Transaction<TransactionController>> dischargeFailureEventHandler) {
        // TODO Auto-generated method stub
        return this;
    }
}
