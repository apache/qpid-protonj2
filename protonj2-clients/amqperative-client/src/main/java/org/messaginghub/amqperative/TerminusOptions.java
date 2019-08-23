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
package org.messaginghub.amqperative;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;

/**
 * Base options type for configuration of {@link Source} and {@link Target} types
 * used by {@link Sender} and {@link Receiver} end points.
 */
public abstract class TerminusOptions {

    /**
     * Control the persistence of source or target state.
     */
    public enum DurabilityMode {
        NONE,
        CONFIGURATION,
        UNSETTLED_STATE
    }

    /**
     * Control when the clock for expiration begins.
     */
    public enum ExpiryPolicy {
        LINK_CLOSE,
        SESSION_CLOSE,
        CONNECTION_CLOSE,
        NEVER
    }

    private String address;
    private DurabilityMode durabilityMode;
    private boolean dynamic;
    private Map<String, Object> dynamicNodeProperties;
    private long timeout;
    private ExpiryPolicy expiryPolicy;
    private String[] capabilities;

    /**
     * @return the address
     */
    public String getAddress() {
        return address;
    }

    /**
     * @param address
     * 		the address to set
     */
    public void setAddress(String address) {
        this.address = address;
    }

    /**
     * @return the durabilityMode
     */
    public DurabilityMode getDurabilityMode() {
        return durabilityMode;
    }

    /**
     * @param durabilityMode the durabilityMode to set
     */
    public void setDurabilityMode(DurabilityMode durabilityMode) {
        this.durabilityMode = durabilityMode;
    }

    /**
     * @return the dynamic
     */
    public boolean isDynamic() {
        return dynamic;
    }

    /**
     * @param dynamic the dynamic to set
     */
    public void setDynamic(boolean dynamic) {
        this.dynamic = dynamic;
    }

    /**
     * @return the dynamicNodeProperties
     */
    public Map<String, Object> getDynamicNodeProperties() {
        return dynamicNodeProperties;
    }

    /**
     * @param dynamicNodeProperties the dynamicNodeProperties to set
     */
    public void setDynamicNodeProperties(Map<String, Object> dynamicNodeProperties) {
        this.dynamicNodeProperties = dynamicNodeProperties;
    }

    /**
     * @return the timeout
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * @param timeout the timeout to set
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * @return the expiryPolicy
     */
    public ExpiryPolicy getExpiryPolicy() {
        return expiryPolicy;
    }

    /**
     * @param expiryPolicy the expiryPolicy to set
     */
    public void setExpiryPolicy(ExpiryPolicy expiryPolicy) {
        this.expiryPolicy = expiryPolicy;
    }

    /**
     * @return the capabilities
     */
    public String[] getCapabilities() {
        return capabilities;
    }

    /**
     * @param capabilities the capabilities to set
     */
    public void setCapabilities(String[] capabilities) {
        this.capabilities = capabilities;
    }

    protected void copyInto(TerminusOptions other) {
        other.setAddress(address);
        other.setDurabilityMode(durabilityMode);
        other.setDynamic(dynamic);
        if (dynamicNodeProperties != null) {
            other.setDynamicNodeProperties(new HashMap<>(dynamicNodeProperties));
        }
        other.setTimeout(timeout);
        other.setExpiryPolicy(expiryPolicy);
        if (capabilities != null) {
            other.setCapabilities(Arrays.copyOf(capabilities, capabilities.length));
        }
    }
}
