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
package org.apache.qpid.proton4j.amqp.transactions;

import java.util.Arrays;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Terminus;

public final class Coordinator implements Terminus {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000030L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:coordinator:list");

    private Symbol[] capabilities;

    public Coordinator() {
        super();
    }

    protected Coordinator(Coordinator other) {
        if (other.capabilities != null) {
            this.capabilities = other.capabilities.clone();
        }
    }

    public Symbol[] getCapabilities() {
        return capabilities;
    }

    public final void setCapabilities(Symbol... capabilities) {
        this.capabilities = capabilities;
    }

    @Override
    public String toString() {
        return "Coordinator{" + "capabilities=" + (getCapabilities() == null ? null : Arrays.asList(getCapabilities())) + '}';
    }

    @Override
    public Coordinator copy() {
        return new Coordinator(this);
    }
}
