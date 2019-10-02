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

import org.apache.qpid.proton4j.amqp.Symbol;

/**
 * Enumeration of all SASL Mechanisms supported by the client, order should be from most secure
 * to least secure.
 */
public enum SaslMechanisms {

    PLAIN {

        private final Symbol name = Symbol.valueOf("PLAIN");

        @Override
        public Symbol symbolicName() {
            return name;
        }

        @Override
        public Mechanism createMechanism() {
            return new PlainMechanism(ordinal());
        }

        @Override
        public boolean isApplicable(SaslCredentialsProvider credentials) {
            return credentials.username() != null && !credentials.username().isEmpty() &&
                   credentials.password() != null && !credentials.password().isEmpty();
        }
    },
    ANONYMOUS {

        private final Symbol name = Symbol.valueOf("ANONYMOUS");

        @Override
        public Symbol symbolicName() {
            return name;
        }

        @Override
        public Mechanism createMechanism() {
            return new AnonymousMechanism(ordinal());
        }

        @Override
        public boolean isApplicable(SaslCredentialsProvider credentials) {
            return true;
        }
    };

    /**
     * @return the {@link Symbol} that represents the {@link Mechanism} name.
     */
    public abstract Symbol symbolicName();

    /**
     * Creates the object that implements the SASL Mechanism represented by this enumeration.
     *
     * @return a new SASL {@link Mechanism} type that will be used for authentication.
     */
    public abstract Mechanism createMechanism();

    /**
     * Allows the Mechanism to determine if it is a valid choice based on the configured
     * credentials at the time of selection.
     *
     * @param credentials
     * 		the login credentials available at the time of mechanism selection.
     *
     * @return true if the mechanism can be used with the provided credentials
     */
    public abstract boolean isApplicable(SaslCredentialsProvider credentials);

    /**
     * Returns the matching {@link SaslMechanisms} enumeration value for the given
     * {@link Symbol} key.
     *
     * @param mechanism
     * 		The symbolic mechanism name to lookup.
     *
     * @return the matching {@link SaslMechanisms} for the given Symbol value.
     */
    public static SaslMechanisms valueOf(Symbol mechanism) {
        return SaslMechanisms.valueOf(mechanism.toString());
    }

    /**
     * Given a mechanism name, validate that it is one of the mechanisms this client supports.
     *
     * @param mechanism
     * 		The mechanism name to validate
     *
     * @return true if the name matches a supported SASL Mechanism
     */
    public static boolean validate(String mechanism) {
        for (SaslMechanisms supported : SaslMechanisms.values()) {
            if (supported.toString().equals(mechanism.toUpperCase())) {
                return true;
            }
        }

        return false;
    }
}
