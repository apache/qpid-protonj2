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
package org.apache.qpid.proton4j.amqp.transport;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;

public abstract class Terminus {

    private String address;
    private TerminusDurability durable = TerminusDurability.NONE;
    private TerminusExpiryPolicy expiryPolicy = TerminusExpiryPolicy.SESSION_END;
    private UnsignedInteger timeout = UnsignedInteger.valueOf(0);
    private boolean dynamic;
    private Map<Symbol, Object> dynamicNodeProperties;
    private Symbol[] capabilities;

    protected Terminus() {
    }

    protected Terminus(Terminus other) {
        this.address = other.address;
        this.durable = other.durable;
        this.expiryPolicy = other.expiryPolicy;
        this.timeout = other.timeout;
        this.dynamic = other.dynamic;

        if (other.dynamicNodeProperties != null) {
            this.dynamicNodeProperties = new HashMap<>(other.dynamicNodeProperties);
        }

        if (other.capabilities != null) {
            this.capabilities = other.capabilities.clone();
        }
    }

    public abstract Terminus copy();

    public final String getAddress() {
        return address;
    }

    public final void setAddress(String address) {
        this.address = address;
    }

    public final TerminusDurability getDurable() {
        return durable;
    }

    public final void setDurable(TerminusDurability durable) {
        this.durable = durable == null ? TerminusDurability.NONE : durable;
    }

    public final TerminusExpiryPolicy getExpiryPolicy() {
        return expiryPolicy;
    }

    public final void setExpiryPolicy(TerminusExpiryPolicy expiryPolicy) {
        this.expiryPolicy = expiryPolicy == null ? TerminusExpiryPolicy.SESSION_END : expiryPolicy;
    }

    public final UnsignedInteger getTimeout() {
        return timeout;
    }

    public final void setTimeout(UnsignedInteger timeout) {
        this.timeout = timeout;
    }

    public final boolean getDynamic() {
        return dynamic;
    }

    public final void setDynamic(boolean dynamic) {
        this.dynamic = dynamic;
    }

    public final Map<Symbol, Object> getDynamicNodeProperties() {
        return dynamicNodeProperties;
    }

    public final void setDynamicNodeProperties(Map<Symbol, Object> dynamicNodeProperties) {
        this.dynamicNodeProperties = dynamicNodeProperties;
    }

    public final Symbol[] getCapabilities() {
        return capabilities;
    }

    public final void setCapabilities(Symbol... capabilities) {
        this.capabilities = capabilities;
    }
}
