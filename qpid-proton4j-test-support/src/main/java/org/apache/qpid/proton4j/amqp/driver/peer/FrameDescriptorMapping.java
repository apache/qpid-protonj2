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
package org.apache.qpid.proton4j.amqp.driver.peer;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.security.SaslChallenge;
import org.apache.qpid.proton4j.amqp.security.SaslInit;
import org.apache.qpid.proton4j.amqp.security.SaslMechanisms;
import org.apache.qpid.proton4j.amqp.security.SaslOutcome;
import org.apache.qpid.proton4j.amqp.security.SaslResponse;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Close;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.End;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.amqp.transport.Transfer;

public class FrameDescriptorMapping {

    private static Map<Object, Object> descriptorMappings = new HashMap<Object, Object>();

    static {
        // AMQP frames (type 0x00)
        addMapping(Open.DESCRIPTOR_SYMBOL, Open.DESCRIPTOR_CODE);
        addMapping(Begin.DESCRIPTOR_SYMBOL, Begin.DESCRIPTOR_CODE);
        addMapping(Attach.DESCRIPTOR_SYMBOL, Attach.DESCRIPTOR_CODE);
        addMapping(Flow.DESCRIPTOR_SYMBOL, Flow.DESCRIPTOR_CODE);
        addMapping(Transfer.DESCRIPTOR_SYMBOL, Transfer.DESCRIPTOR_CODE);
        addMapping(Disposition.DESCRIPTOR_SYMBOL, Disposition.DESCRIPTOR_CODE);
        addMapping(Detach.DESCRIPTOR_SYMBOL, Detach.DESCRIPTOR_CODE);
        addMapping(End.DESCRIPTOR_SYMBOL, End.DESCRIPTOR_CODE);
        addMapping(Close.DESCRIPTOR_SYMBOL, Close.DESCRIPTOR_CODE);

        // SASL frames (type 0x01)
        addMapping(SaslMechanisms.DESCRIPTOR_SYMBOL, SaslMechanisms.DESCRIPTOR_CODE);
        addMapping(SaslInit.DESCRIPTOR_SYMBOL, SaslInit.DESCRIPTOR_CODE);
        addMapping(SaslChallenge.DESCRIPTOR_SYMBOL, SaslChallenge.DESCRIPTOR_CODE);
        addMapping(SaslResponse.DESCRIPTOR_SYMBOL, SaslResponse.DESCRIPTOR_CODE);
        addMapping(SaslOutcome.DESCRIPTOR_SYMBOL, SaslOutcome.DESCRIPTOR_CODE);
    }

    private static void addMapping(Symbol descriptorSymbol, UnsignedLong descriptorCode) {
        descriptorMappings.put(descriptorSymbol, descriptorCode);
        descriptorMappings.put(descriptorCode, descriptorSymbol);
    }

    public static Object lookupMapping(Object descriptor) {
        Object mapping = descriptorMappings.get(descriptor);
        if (mapping == null) {
            mapping = "UNKNOWN";
        }

        return mapping;
    }
}
