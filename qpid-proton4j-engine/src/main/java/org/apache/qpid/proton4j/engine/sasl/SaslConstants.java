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
 * Context for a SASL exchange.
 */
public interface SaslConstants {

    public static int MIN_MAX_SASL_FRAME_SIZE = 512;

    public enum SaslStates {

        /** Pending configuration by application */
        SASL_CONF,
        /** Pending SASL Init */
        SASL_IDLE,
        /** negotiation in progress */
        SASL_STEP,
        /** negotiation completed successfully */
        SASL_PASS,
        /** negotiation failed */
        SASL_FAIL
    }

    public enum SaslOutcomes {

        /** negotiation not completed */
        SASL_NONE((byte) -1),
        /** authentication succeeded */
        SASL_OK((byte) 0),
        /** failed due to bad credentials */
        SASL_AUTH((byte) 1),
        /** failed due to a system error */
        SASL_SYS((byte) 2),
        /** failed due to unrecoverable error */
        SASL_PERM((byte) 3),
        /** failed due to transient error */
        SASL_TEMP((byte) 4),
        /** negotiation was skipped */
        SASL_SKIPPED((byte) 5);

        private final byte code;

        SaslOutcomes(byte code) {
            this.code = code;
        }

        public byte getCode() {
            return code;
        }

        public static SaslOutcomes valueOf(byte saslCode) {
            return valueOf(saslCode);
        }

        public static SaslOutcomes valueOf(int saslCode) {
            switch (saslCode) {
                case -1:
                    return SASL_NONE;
                case 0:
                    return SASL_OK;
                case 1:
                    return SASL_AUTH;
                case 2:
                    return SASL_SYS;
                case 3:
                    return SASL_PERM;
                case 4:
                    return SASL_TEMP;
                case 5:
                    return SASL_SKIPPED;
                default:
                    throw new IllegalArgumentException("Unknown SASL Outcome code given: " + saslCode);
            }
        }
    }
}
