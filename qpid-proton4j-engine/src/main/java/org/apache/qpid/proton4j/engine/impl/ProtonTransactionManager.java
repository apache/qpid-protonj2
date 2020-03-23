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
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.amqp.transactions.Declare;
import org.apache.qpid.proton4j.amqp.transactions.Discharge;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.LinkState;
import org.apache.qpid.proton4j.engine.Session;
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
    public TransactionManager detach() {
        // TODO: Detach on a coordinator doesn't make a ton of sense, should be error or just close ?
        receiverLink.detach();
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
    public boolean isLocallyDetached() {
        return receiverLink.isLocallyDetached();
    }

    @Override
    public LinkState getState() {
        return receiverLink.getState();
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
    public int getCredit() {
        return receiverLink.getCredit();
    }

    @Override
    public boolean isDraining() {
        return receiverLink.isDraining();
    }

    @Override
    public Role getRole() {
        return Role.RECEIVER;
    }

    @Override
    public boolean isSender() {
        return false;
    }

    @Override
    public boolean isReceiver() {
        return true;
    }

    @Override
    public Connection getConnection() {
        return receiverLink.getConnection();
    }

    @Override
    public Session getSession() {
        return receiverLink.getSession();
    }

    @Override
    public String getName() {
        return receiverLink.getName();
    }

    @Override
    public TransactionManager setSenderSettleMode(SenderSettleMode senderSettleMode) throws IllegalStateException {
        receiverLink.setSenderSettleMode(senderSettleMode);
        return this;
    }

    @Override
    public SenderSettleMode getSenderSettleMode() {
        return receiverLink.getSenderSettleMode();
    }

    @Override
    public TransactionManager setReceiverSettleMode(ReceiverSettleMode receiverSettleMode) throws IllegalStateException {
        receiverLink.setReceiverSettleMode(receiverSettleMode);
        return this;
    }

    @Override
    public ReceiverSettleMode getReceiverSettleMode() {
        return receiverLink.getReceiverSettleMode();
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
    public TransactionManager setTarget(Target target) throws IllegalStateException {
        receiverLink.setTarget(target);
        return this;
    }

    @Override
    public Target getTarget() {
        return receiverLink.getTarget();
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
    public TransactionManager setMaxMessageSize(UnsignedLong maxMessageSize) throws IllegalStateException {
        receiverLink.setMaxMessageSize(maxMessageSize);
        return this;
    }

    @Override
    public UnsignedLong getMaxMessageSize() {
        return receiverLink.getMaxMessageSize();
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
    public boolean isRemotelyDetached() {
        return receiverLink.isRemotelyDetached();
    }

    @Override
    public Source getRemoteSource() {
        return receiverLink.getRemoteSource();
    }

    @Override
    public Target getRemoteTarget() {
        return receiverLink.getRemoteTarget();
    }

    @Override
    public SenderSettleMode getRemoteSenderSettleMode() {
        return receiverLink.getRemoteSenderSettleMode();
    }

    @Override
    public ReceiverSettleMode getRemoteReceiverSettleMode() {
        return receiverLink.getRemoteReceiverSettleMode();
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
    public UnsignedLong getRemoteMaxMessageSize() {
        return receiverLink.getRemoteMaxMessageSize();
    }

    @Override
    public LinkState getRemoteState() {
        return receiverLink.getRemoteState();
    }

    @Override
    public ErrorCondition getRemoteCondition() {
        return receiverLink.getRemoteCondition();
    }

    @Override
    public TransactionManager localDetachHandler(EventHandler<TransactionManager> localDetachHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionManager detachHandler(EventHandler<TransactionManager> remoteDetachHandler) {
        // TODO Auto-generated method stub
        return this;
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
