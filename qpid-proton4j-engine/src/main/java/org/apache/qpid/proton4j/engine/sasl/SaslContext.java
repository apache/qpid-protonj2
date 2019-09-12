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
package org.apache.qpid.proton4j.engine.sasl;

/**
 * The basic SASL context APIs common to both client and server sides of the SASL exchange.
 */
public interface SaslContext {

    enum Role { CLIENT, SERVER }

    /**
     * Return the Role of the context implementation.
     *
     * @return the Role of this SASL Context
     */
    Role getRole();

    /**
     * @return true if SASL authentication has completed
     */
    boolean isDone();

    /**
     * @return true if this is a SASL server context.
     */
    default boolean isServer() {
        return getRole() == Role.SERVER;
    }

    /**
     * @return true if this is a SASL client context.
     */
    default boolean isClient() {
        return getRole() == Role.SERVER;
    }
}
