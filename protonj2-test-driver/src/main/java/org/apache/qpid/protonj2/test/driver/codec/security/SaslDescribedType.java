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
package org.apache.qpid.protonj2.test.driver.codec.security;

import java.util.List;

import org.apache.qpid.protonj2.test.driver.codec.ListDescribedType;

/**
 * SASL Types base class used to mark types that are SASL related
 */
public abstract class SaslDescribedType extends ListDescribedType {

    public enum SaslPerformativeType {
        INIT,
        MECHANISMS,
        CHALLENGE,
        RESPONSE,
        OUTCOME
    }

    public SaslDescribedType(int numberOfFields) {
        super(numberOfFields);
    }

    public SaslDescribedType(int numberOfFields, List<Object> described) {
        super(numberOfFields, described);
    }

    public abstract SaslPerformativeType getPerformativeType();

    public interface SaslPerformativeHandler<E> {

        default void handleMechanisms(SaslMechanisms saslMechanisms, E context) {
            throw new AssertionError("SASL Mechanisms was not handled");
        }
        default void handleInit(SaslInit saslInit, E context) {
            throw new AssertionError("SASL Init was not handled");
        }
        default void handleChallenge(SaslChallenge saslChallenge, E context) {
            throw new AssertionError("SASL Challenge was not handled");
        }
        default void handleResponse(SaslResponse saslResponse, E context) {
            throw new AssertionError("SASL Response was not handled");
        }
        default void handleOutcome(SaslOutcome saslOutcome, E context) {
            throw new AssertionError("SASL Outcome was not handled");
        }
    }

    public abstract <E> void invoke(SaslPerformativeHandler<E> handler, E context);

}
