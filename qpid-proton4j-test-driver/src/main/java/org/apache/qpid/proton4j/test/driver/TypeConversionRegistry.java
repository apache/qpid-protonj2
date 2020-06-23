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
package org.apache.qpid.proton4j.test.driver;

/**
 * Registry of external application converters from an external AMQP type to the internal test
 * driver types.
 */
public class TypeConversionRegistry {

    public enum AMQPTypes {
        BINARY,
        DECIMAL128,
        DECIMAL64,
        DECIMAL32,
        SYMBOL,
        UNSIGNED_BYTE,
        UNSIGNED_SHORT,
        UNSIGNED_INT,
        UNSIGNED_LONG,
        ACCEPTED,
        AMQP_SEQUENCE,
        AMQP_VALUE,
        APPLICATION_PROPERTIES,
        DATA,
        DELETE_ON_CLOSE,
        DELETE_ON_NO_LINKS,
        DELETE_ON_NO_LINKS_OR_NO_MESSAGES,
        DELETE_ON_NO_MESSAGES,
        DELIVERY_ANNOTATIONS,
        FOOTER,
        HEADER,
        MESSAGE_ANNOTATIONS,
        MODIFIED,
        PROPERTIES,
        RECEIVED,
        REJECTED,
        RELEASED,
        SOURCE,
        TARGET,
        ATTACH,
        BEGIN,
        CLOSE,
        DETACH,
        DISPOSITION,
        END,
        ERROR_CONDITION,
        FLOW,
        OPEN,
        TRANSFER,
        COORDINATOR,
        DECLARE,
        DECLARED,
        DISCHARGE,
        TRANSACTIONAL_STATE,
        SASL_CHALLENGE,
        SASL_INIT,
        SASL_MECHANISM,
        SASL_OUTCOME,
        SASL_RESPONSE
    }

    TypeConversionRegistry() {
        // TODO Auto-generated constructor stub
    }

}
