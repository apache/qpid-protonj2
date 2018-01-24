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
package org.apache.qpid.proton4j.transport.sasl;

/**
 * Context for a SASL exchange.
 */
public interface SaslConstants {

    // TODO - PN_ is a C'ism, can we do away with that ?

    public enum SaslStates {
        /** Pending configuration by application */
        PN_SASL_CONF,
        /** Pending SASL Init */
        PN_SASL_IDLE,
        /** negotiation in progress */
        PN_SASL_STEP,
        /** negotiation completed successfully */
        PN_SASL_PASS,
        /** negotiation failed */
        PN_SASL_FAIL
    }

    public enum SaslOutcomes {
        /** negotiation not completed */
        PN_SASL_NONE((byte) -1),
        /** authentication succeeded */
        PN_SASL_OK((byte) 0),
        /** failed due to bad credentials */
        PN_SASL_AUTH((byte) 1),
        /** failed due to a system error */
        PN_SASL_SYS((byte) 2),
        /** failed due to unrecoverable error */
        PN_SASL_PERM((byte) 3), PN_SASL_TEMP((byte) 4), PN_SASL_SKIPPED((byte) 5);

        private final byte _code;

        /** failed due to transient error */

        SaslOutcomes(byte code) {
            _code = code;
        }

        public byte getCode() {
            return _code;
        }
    }

    public static SaslOutcomes PN_SASL_NONE = SaslOutcomes.PN_SASL_NONE;
    public static SaslOutcomes PN_SASL_OK = SaslOutcomes.PN_SASL_OK;
    public static SaslOutcomes PN_SASL_AUTH = SaslOutcomes.PN_SASL_AUTH;
    public static SaslOutcomes PN_SASL_SYS = SaslOutcomes.PN_SASL_SYS;
    public static SaslOutcomes PN_SASL_PERM = SaslOutcomes.PN_SASL_PERM;
    public static SaslOutcomes PN_SASL_TEMP = SaslOutcomes.PN_SASL_TEMP;
    public static SaslOutcomes PN_SASL_SKIPPED = SaslOutcomes.PN_SASL_SKIPPED;

}
