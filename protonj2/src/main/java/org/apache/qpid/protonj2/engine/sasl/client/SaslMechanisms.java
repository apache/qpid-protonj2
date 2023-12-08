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
package org.apache.qpid.protonj2.engine.sasl.client;

import org.apache.qpid.protonj2.types.Symbol;

/**
 * Enumeration of all SASL Mechanisms supported by the client, order should be from most secure
 * to least secure.
 */
public enum SaslMechanisms {

    EXTERNAL {

        private final Mechanism INSTANCE = new ExternalMechanism();

        @Override
        public Symbol getName() {
            return ExternalMechanism.EXTERNAL;
        }

        @Override
        public Mechanism createMechanism() {
            return INSTANCE;
        }
    },
    SCRAM_SHA_512 {

        @Override
        public Symbol getName() {
            return ScramSHA512Mechanism.SCRAM_SHA_512;
        }

        @Override
        public Mechanism createMechanism() {
            return new ScramSHA512Mechanism();
        }
    },
    SCRAM_SHA_256 {

        @Override
        public Symbol getName() {
            return ScramSHA256Mechanism.SCRAM_SHA_256;
        }

        @Override
        public Mechanism createMechanism() {
            return new ScramSHA256Mechanism();
        }
    },
    SCRAM_SHA_1 {

        @Override
        public Symbol getName() {
            return ScramSHA1Mechanism.SCRAM_SHA_1;
        }

        @Override
        public Mechanism createMechanism() {
            return new ScramSHA1Mechanism();
        }
    },
    CRAM_MD5 {

        @Override
        public Symbol getName() {
            return CramMD5Mechanism.CRAM_MD5;
        }

        @Override
        public Mechanism createMechanism() {
            return new CramMD5Mechanism();
        }
    },
    PLAIN {

        private final Mechanism INSTANCE = new PlainMechanism();

        @Override
        public Symbol getName() {
            return PlainMechanism.PLAIN;
        }

        @Override
        public Mechanism createMechanism() {
            return INSTANCE;
        }
    },
    XOAUTH2 {

        @Override
        public Symbol getName() {
            return XOauth2Mechanism.XOAUTH2;
        }

        @Override
        public Mechanism createMechanism() {
            return new XOauth2Mechanism();
        }
    },
    ANONYMOUS {

        private final Mechanism INSTANCE = new AnonymousMechanism();

        @Override
        public Symbol getName() {
            return AnonymousMechanism.ANONYMOUS;
        }

        @Override
        public Mechanism createMechanism() {
            return INSTANCE;
        }
    };

    /**
     * @return the {@link Symbol} that represents the {@link Mechanism} name.
     */
    public abstract Symbol getName();

    /**
     * Creates the object that implements the SASL Mechanism represented by this enumeration.
     *
     * @return a new SASL {@link Mechanism} type that will be used for authentication.
     */
    public abstract Mechanism createMechanism();

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
        for (SaslMechanisms value : SaslMechanisms.values()) {
            if (value.getName().equals(mechanism)) {
                return value;
            }
        }

        throw new IllegalArgumentException("No Matching SASL Mechanism with name: " + mechanism);
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
            if (supported.getName().toString().equals(mechanism.toUpperCase())) {
                return true;
            }
        }

        return false;
    }
}
