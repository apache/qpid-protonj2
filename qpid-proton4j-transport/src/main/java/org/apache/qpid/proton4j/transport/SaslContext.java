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
package org.apache.qpid.proton4j.transport;

/**
 * Context for a SASL exchange.
 */
public interface SaslContext {

    // TODO - PN_ is a C'ism, can we do away with that ?

    public enum SaslState {
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

    public enum SaslOutcome {
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

        SaslOutcome(byte code) {
            _code = code;
        }

        public byte getCode() {
            return _code;
        }
    }

    public static SaslOutcome PN_SASL_NONE = SaslOutcome.PN_SASL_NONE;
    public static SaslOutcome PN_SASL_OK = SaslOutcome.PN_SASL_OK;
    public static SaslOutcome PN_SASL_AUTH = SaslOutcome.PN_SASL_AUTH;
    public static SaslOutcome PN_SASL_SYS = SaslOutcome.PN_SASL_SYS;
    public static SaslOutcome PN_SASL_PERM = SaslOutcome.PN_SASL_PERM;
    public static SaslOutcome PN_SASL_TEMP = SaslOutcome.PN_SASL_TEMP;
    public static SaslOutcome PN_SASL_SKIPPED = SaslOutcome.PN_SASL_SKIPPED;

}
