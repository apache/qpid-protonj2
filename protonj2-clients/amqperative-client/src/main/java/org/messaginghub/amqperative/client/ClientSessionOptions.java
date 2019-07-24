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
package org.messaginghub.amqperative.client;

import java.util.List;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;

/**
 * TODO
 */
public class ClientSessionOptions {

    private long handleMax = -1;
    private Map<Symbol, Object> properties;
    private List<Symbol> offeredCapabilities;
    private List<Symbol> desiredCapabilities;

    public ClientSessionOptions copyInto(ClientSessionOptions options) {
        return this;
    }

    /**
     * @return the handleMax
     */
    public long getHandleMax() {
        return handleMax;
    }

    /**
     * @param handleMax the handleMax to set
     */
    public void setHandleMax(long handleMax) {
        this.handleMax = handleMax;
    }

    /**
     * @return the properties
     */
    public Map<Symbol, Object> getProperties() {
        return properties;
    }

    /**
     * @param properties the properties to set
     */
    public void setProperties(Map<Symbol, Object> properties) {
        this.properties = properties;
    }

    /**
     * @return the offeredCapabilities
     */
    public List<Symbol> getOfferedCapabilities() {
        return offeredCapabilities;
    }

    /**
     * @param offeredCapabilities the offeredCapabilities to set
     */
    public void setOfferedCapabilities(List<Symbol> offeredCapabilities) {
        this.offeredCapabilities = offeredCapabilities;
    }

    /**
     * @return the desiredCapabilities
     */
    public List<Symbol> getDesiredCapabilities() {
        return desiredCapabilities;
    }

    /**
     * @param desiredCapabilities the desiredCapabilities to set
     */
    public void setDesiredCapabilities(List<Symbol> desiredCapabilities) {
        this.desiredCapabilities = desiredCapabilities;
    }
}
