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

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.transactions.Coordinator;
import org.apache.qpid.proton4j.amqp.transactions.Declare;
import org.apache.qpid.proton4j.amqp.transactions.Discharge;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.Transaction;
import org.apache.qpid.proton4j.engine.TransactionManager;
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;

/**
 * {@link TransactionManager} implementation that implements the abstraction
 * around a receiver link that responds to requests to {@link Declare} and to
 * {@link Discharge} AMQP {@link Transaction} instance.
 */
public class ProtonTransactionManager extends ProtonEndpoint<TransactionManager> implements TransactionManager {

    private final ProtonReceiver receiverLink;

    public ProtonTransactionManager(ProtonReceiver receiverLink) {
        super(receiverLink.getEngine());

        this.receiverLink = receiverLink;
    }

    @Override
    public ProtonSession getParent() {
        return receiverLink.getSession();
    }

    @Override
    ProtonTransactionManager self() {
        return this;
    }

    @Override
    public TransactionManager open() throws IllegalStateException, EngineStateException {
        receiverLink.open();
        return this;
    }

    @Override
    public TransactionManager close() throws EngineFailedException {
        receiverLink.close();
        return this;
    }

    @Override
    public boolean isLocallyOpen() {
        return receiverLink.isLocallyOpen();
    }

    @Override
    public boolean isLocallyClosed() {
        return receiverLink.isLocallyClosed();
    }

    @Override
    public TransactionManager setSource(Source source) throws IllegalStateException {
        receiverLink.setSource(source);
        return this;
    }

    @Override
    public Source getSource() {
        return receiverLink.getSource();
    }

    @Override
    public TransactionManager setCoordinator(Coordinator coordinator) throws IllegalStateException {
        receiverLink.setTarget(coordinator);
        return this;
    }

    @Override
    public Coordinator getCoordinator() {
        return receiverLink.getTarget();
    }

    @Override
    public ErrorCondition getCondition() {
        return receiverLink.getCondition();
    }

    @Override
    public TransactionManager setCondition(ErrorCondition condition) {
        receiverLink.setCondition(condition);
        return this;
    }

    @Override
    public Map<Symbol, Object> getProperties() {
        return receiverLink.getProperties();
    }

    @Override
    public TransactionManager setProperties(Map<Symbol, Object> properties) throws IllegalStateException {
        receiverLink.setProperties(properties);
        return this;
    }

    @Override
    public TransactionManager setOfferedCapabilities(Symbol... offeredCapabilities) throws IllegalStateException {
        receiverLink.setOfferedCapabilities(offeredCapabilities);
        return this;
    }

    @Override
    public Symbol[] getOfferedCapabilities() {
        return receiverLink.getOfferedCapabilities();
    }

    @Override
    public TransactionManager setDesiredCapabilities(Symbol... desiredCapabilities) throws IllegalStateException {
        receiverLink.setDesiredCapabilities(desiredCapabilities);
        return this;
    }

    @Override
    public Symbol[] getDesiredCapabilities() {
        return receiverLink.getDesiredCapabilities();
    }

    @Override
    public boolean isRemotelyOpen() {
        return receiverLink.isRemotelyOpen();
    }

    @Override
    public boolean isRemotelyClosed() {
        return receiverLink.isRemotelyClosed();
    }

    @Override
    public Symbol[] getRemoteOfferedCapabilities() {
        return receiverLink.getRemoteOfferedCapabilities();
    }

    @Override
    public Symbol[] getRemoteDesiredCapabilities() {
        return receiverLink.getRemoteDesiredCapabilities();
    }

    @Override
    public Map<Symbol, Object> getRemoteProperties() {
        return receiverLink.getRemoteProperties();
    }

    @Override
    public ErrorCondition getRemoteCondition() {
        return receiverLink.getRemoteCondition();
    }

    @Override
    public Source getRemoteSource() {
        return receiverLink.getRemoteSource();
    }

    @Override
    public Coordinator getRemoteCoordinator() {
        return receiverLink.getRemoteTarget();
    }

    @Override
    public TransactionManager engineShutdownHandler(EventHandler<Engine> engineShutdownEventHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionManager declared(Transaction<TransactionManager> transaction, Binary txnId) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionManager discharged(Transaction<TransactionManager> transaction) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionManager declare(EventHandler<Transaction<TransactionManager>> declaredEventHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionManager discharge(EventHandler<Transaction<TransactionManager>> declaredEventHandler) {
        // TODO Auto-generated method stub
        return this;
    }
}
