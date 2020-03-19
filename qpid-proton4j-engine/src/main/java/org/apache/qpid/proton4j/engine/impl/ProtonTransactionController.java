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
import org.apache.qpid.proton4j.engine.Context;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.LinkState;
import org.apache.qpid.proton4j.engine.Session;
import org.apache.qpid.proton4j.engine.Transaction;
import org.apache.qpid.proton4j.engine.TransactionController;
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;

/**
 * {@link TransactionController} implementation that implements the abstraction
 * around a sender link that initiates requests to {@link Declare} and to
 * {@link Discharge} AMQP {@link Transaction} instance.
 */
public class ProtonTransactionController implements TransactionController {

    private final ProtonSender senderLink;

    public ProtonTransactionController(ProtonSender senderLink) {
        this.senderLink = senderLink;
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
    public TransactionController detach() {
        // TODO: Detach on a coordinator doesn't make a ton of sense, should be error or just close ?
        senderLink.detach();
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
    public boolean isLocallyDetached() {
        senderLink.isLocallyDetached();
        return false;
    }

    @Override
    public Context getContext() {
        return senderLink.getContext();
    }

    @Override
    public LinkState getState() {
        return senderLink.getState();
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
    public int getCredit() {
        return senderLink.getCredit();
    }

    @Override
    public boolean isDraining() {
        return senderLink.isDraining();
    }

    @Override
    public Role getRole() {
        return Role.SENDER;
    }

    @Override
    public boolean isSender() {
        return true;
    }

    @Override
    public boolean isReceiver() {
        return false;
    }

    @Override
    public Connection getConnection() {
        return senderLink.getConnection();
    }

    @Override
    public Session getSession() {
        return senderLink.getSession();
    }

    @Override
    public Engine getEngine() {
        return senderLink.getEngine();
    }

    @Override
    public String getName() {
        return senderLink.getName();
    }

    @Override
    public TransactionController setSenderSettleMode(SenderSettleMode senderSettleMode) throws IllegalStateException {
        senderLink.setSenderSettleMode(senderSettleMode);
        return this;
    }

    @Override
    public SenderSettleMode getSenderSettleMode() {
        return senderLink.getSenderSettleMode();
    }

    @Override
    public TransactionController setReceiverSettleMode(ReceiverSettleMode receiverSettleMode) throws IllegalStateException {
        senderLink.setReceiverSettleMode(receiverSettleMode);
        return this;
    }

    @Override
    public ReceiverSettleMode getReceiverSettleMode() {
        return senderLink.getReceiverSettleMode();
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
    public TransactionController setTarget(Target target) throws IllegalStateException {
        senderLink.setTarget(target);
        return this;
    }

    @Override
    public Target getTarget() {
        return senderLink.getTarget();
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
    public TransactionController setMaxMessageSize(UnsignedLong maxMessageSize) throws IllegalStateException {
        senderLink.setMaxMessageSize(maxMessageSize);
        return this;
    }

    @Override
    public UnsignedLong getMaxMessageSize() {
        return senderLink.getMaxMessageSize();
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
    public boolean isRemotelyDetached() {
        return senderLink.isRemotelyDetached();
    }

    @Override
    public Source getRemoteSource() {
        return senderLink.getRemoteSource();
    }

    @Override
    public Target getRemoteTarget() {
        return senderLink.getRemoteTarget();
    }

    @Override
    public SenderSettleMode getRemoteSenderSettleMode() {
        return senderLink.getRemoteSenderSettleMode();
    }

    @Override
    public ReceiverSettleMode getRemoteReceiverSettleMode() {
        return senderLink.getRemoteReceiverSettleMode();
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
    public UnsignedLong getRemoteMaxMessageSize() {
        return senderLink.getRemoteMaxMessageSize();
    }

    @Override
    public LinkState getRemoteState() {
        return senderLink.getRemoteState();
    }

    @Override
    public ErrorCondition getRemoteCondition() {
        return senderLink.getRemoteCondition();
    }

    @Override
    public TransactionController localOpenHandler(EventHandler<TransactionController> localOpenHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionController localCloseHandler(EventHandler<TransactionController> localCloseHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionController localDetachHandler(EventHandler<TransactionController> localDetachHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionController openHandler(EventHandler<TransactionController> remoteOpenHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionController detachHandler(EventHandler<TransactionController> remoteDetachHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionController closeHandler(EventHandler<TransactionController> remoteCloseHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public TransactionController engineShutdownHandler(EventHandler<Engine> engineShutdownEventHandler) {
        // TODO Auto-generated method stub
        return this;
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
