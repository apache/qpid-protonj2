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
import org.apache.qpid.proton4j.engine.Link;
import org.apache.qpid.proton4j.engine.Session;

/**
 * Common base for Proton Senders and Receivers.
 */
public abstract class ProtonLink extends ProtonEndpoint implements Link {

    @Override
    public Session getSession() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Source getSource() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Target getTarget() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<Symbol, Object> getProperties() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setOfferedCapabilities(Symbol[] offeredCapabilities) {
        // TODO Auto-generated method stub
    }

    @Override
    public Symbol[] getOfferedCapabilities() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setDesiredCapabilities(Symbol[] desiredCapabilities) {
        // TODO Auto-generated method stub
    }

    @Override
    public Symbol[] getDesiredCapabilities() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setProperties(Map<Symbol, Object> properties) {
        // TODO Auto-generated method stub
    }

    @Override
    public void setMaxMessageSize(UnsignedLong maxMessageSize) {
        // TODO Auto-generated method stub
    }

    @Override
    public UnsignedLong getMaxMessageSize() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Source getRemoteSource() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Target getRemoteTarget() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Symbol[] getRemoteOfferedCapabilities() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Symbol[] getRemoteDesiredCapabilities() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<Symbol, Object> getRemoteProperties() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UnsignedLong getRemoteMaxMessageSize() {
        // TODO Auto-generated method stub
        return null;
    }
}
