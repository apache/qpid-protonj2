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

public final class Source implements Terminus {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000028L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:source:list");

    private String address;
    private TerminusDurability durable = TerminusDurability.NONE;
    private TerminusExpiryPolicy expiryPolicy = TerminusExpiryPolicy.SESSION_END;
    private UnsignedInteger timeout = UnsignedInteger.valueOf(0);
    private boolean dynamic;
    private Map<Symbol, Object> dynamicNodeProperties;
    private Symbol distributionMode;
    private Map<Symbol, Object> filter;
    private Outcome defaultOutcome;
    private Symbol[] outcomes;
    private Symbol[] capabilities;

    public Source() {
    }

    private Source(Source other) {
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

        this.distributionMode = other.distributionMode;

        if (other.filter != null) {
            this.filter = new HashMap<>(other.filter);
        }

        this.defaultOutcome = other.defaultOutcome;

        if (other.outcomes != null) {
            this.outcomes = other.outcomes.clone();
        }
    }

    @Override
    public Source copy() {
        return new Source(this);
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

    public Symbol getDistributionMode() {
        return distributionMode;
    }

    public void setDistributionMode(Symbol distributionMode) {
        this.distributionMode = distributionMode;
    }

    public Map<Symbol, Object> getFilter() {
        return filter;
    }

    public void setFilter(Map<Symbol, Object> filter) {
        this.filter = filter;
    }

    public Outcome getDefaultOutcome() {
        return defaultOutcome;
    }

    public void setDefaultOutcome(Outcome defaultOutcome) {
        this.defaultOutcome = defaultOutcome;
    }

    public Symbol[] getOutcomes() {
        return outcomes;
    }

    public void setOutcomes(Symbol... outcomes) {
        this.outcomes = outcomes;
    }

    @Override
    public String toString() {
        return "Source{" +
               "address='" + getAddress() + '\'' +
               ", durable=" + getDurable() +
               ", expiryPolicy=" + getExpiryPolicy() +
               ", timeout=" + getTimeout() +
               ", dynamic=" + getDynamic() +
               ", dynamicNodeProperties=" + getDynamicNodeProperties() +
               ", distributionMode=" + distributionMode +
               ", filter=" + filter +
               ", defaultOutcome=" + defaultOutcome +
               ", outcomes=" + (outcomes == null ? null : Arrays.asList(outcomes)) +
               ", capabilities=" + (getCapabilities() == null ? null : Arrays.asList(getCapabilities())) +
               '}';
    }
}
