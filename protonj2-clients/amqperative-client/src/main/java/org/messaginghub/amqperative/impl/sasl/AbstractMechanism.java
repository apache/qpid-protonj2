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
package org.messaginghub.amqperative.impl.sasl;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;

/**
 * Base class for SASL Authentication Mechanism that implements the basic
 * methods of a Mechanism class.
 */
public abstract class AbstractMechanism implements Mechanism {

    protected static final ProtonBuffer EMPTY = ProtonByteBufferAllocator.DEFAULT.allocate(0, 0);

    private String username;
    private String password;

    @Override
    public void verifyCompletion() {
    }

    @Override
    public ProtonBuffer getInitialResponse() {
        return null;
    }

    @Override
    public ProtonBuffer getChallengeResponse(ProtonBuffer challenge) {
        return EMPTY;
    }

    @Override
    public void setUsername(String value) {
        this.username = value;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public void setPassword(String value) {
        this.password = value;
    }

    @Override
    public String getPassword() {
        return this.password;
    }

    @Override
    public String toString() {
        return "SASL-" + getName();
    }
}
