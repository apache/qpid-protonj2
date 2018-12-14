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
package org.apache.qpid.proton4j.amqp.messaging;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;

public final class Target implements Terminus {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000029L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:target:list");

    private String address;
    private TerminusDurability durable = TerminusDurability.NONE;
    private TerminusExpiryPolicy expiryPolicy = TerminusExpiryPolicy.SESSION_END;
    private UnsignedInteger timeout = UnsignedInteger.ZERO;
    private boolean dynamic;
    private Map<Symbol, Object> dynamicNodeProperties;
    private Symbol[] capabilities;

    public Target() {
    }

    protected Target(Target other) {
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

    @Override
    public Target copy() {
        return new Target(this);
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public TerminusDurability getDurable() {
        return durable;
    }

    public void setDurable(TerminusDurability durable) {
        this.durable = durable == null ? TerminusDurability.NONE : durable;
    }

    public TerminusExpiryPolicy getExpiryPolicy() {
        return expiryPolicy;
    }

    public void setExpiryPolicy(TerminusExpiryPolicy expiryPolicy) {
        this.expiryPolicy = expiryPolicy == null ? TerminusExpiryPolicy.SESSION_END : expiryPolicy;
    }

    public UnsignedInteger getTimeout() {
        return timeout;
    }

    public void setTimeout(UnsignedInteger timeout) {
        this.timeout = timeout;
    }

    public boolean getDynamic() {
        return dynamic;
    }

    public void setDynamic(boolean dynamic) {
        this.dynamic = dynamic;
    }

    public Map<Symbol, Object> getDynamicNodeProperties() {
        return dynamicNodeProperties;
    }

    public void setDynamicNodeProperties(Map<Symbol, Object> dynamicNodeProperties) {
        this.dynamicNodeProperties = dynamicNodeProperties;
    }

    public Symbol[] getCapabilities() {
        return capabilities;
    }

    public final void setCapabilities(Symbol... capabilities) {
        this.capabilities = capabilities;
    }

    @Override
    public String toString() {
        return "Target{" +
               "address='" + getAddress() + '\'' +
               ", durable=" + getDurable() +
               ", expiryPolicy=" + getExpiryPolicy() +
               ", timeout=" + getTimeout() +
               ", dynamic=" + getDynamic() +
               ", dynamicNodeProperties=" + getDynamicNodeProperties() +
               ", capabilities=" + (getCapabilities() == null ? null : Arrays.asList(getCapabilities())) +
               '}';
    }
}
