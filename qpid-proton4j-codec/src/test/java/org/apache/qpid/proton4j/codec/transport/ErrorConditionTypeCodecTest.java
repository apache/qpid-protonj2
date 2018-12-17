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
package org.apache.qpid.proton4j.codec.transport;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.junit.Test;

public class ErrorConditionTypeCodecTest extends CodecTestSupport {

    @Test
    public void testEncodeDecodeType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        ErrorCondition error = new ErrorCondition();
        error.setCondition(Symbol.valueOf("amqp-error"));
        error.setDescription("Something bad");

        Map<Object, Object> infoMap = new LinkedHashMap<>();
        infoMap.put("1", true);
        infoMap.put("2", "string");

        error.setInfo(infoMap);

        encoder.writeObject(buffer, encoderState, error);

        final ErrorCondition result = (ErrorCondition) decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertNotNull(result.getCondition());
        assertNotNull(result.getDescription());
        assertNotNull(result.getInfo());

        assertEquals(Symbol.valueOf("amqp-error"), result.getCondition());
        assertEquals("Something bad", result.getDescription());
        assertEquals(infoMap, result.getInfo());
    }

    @Test
    public void testEqualityOfNewlyConstructed() {
        ErrorCondition new1 = new ErrorCondition();
        ErrorCondition new2 = new ErrorCondition();
        assertErrorConditionsEqual(new1, new2);
    }

    @Test
    public void testSameObject() {
        ErrorCondition error = new ErrorCondition();
        assertErrorConditionsEqual(error, error);
    }

    @Test
    public void testConditionEquality() {
        String symbolValue = "symbol";

        ErrorCondition same1 = new ErrorCondition();
        same1.setCondition(Symbol.getSymbol(new String(symbolValue)));

        ErrorCondition same2 = new ErrorCondition();
        same2.setCondition(Symbol.getSymbol(new String(symbolValue)));

        assertErrorConditionsEqual(same1, same2);

        ErrorCondition different = new ErrorCondition();
        different.setCondition(Symbol.getSymbol("other"));

        assertErrorConditionsNotEqual(same1, different);
    }

    @Test
    public void testConditionAndDescriptionEquality() {
        String symbolValue = "symbol";
        String descriptionValue = "description";

        ErrorCondition same1 = new ErrorCondition();
        same1.setCondition(Symbol.getSymbol(new String(symbolValue)));
        same1.setDescription(new String(descriptionValue));

        ErrorCondition same2 = new ErrorCondition();
        same2.setCondition(Symbol.getSymbol(new String(symbolValue)));
        same2.setDescription(new String(descriptionValue));

        assertErrorConditionsEqual(same1, same2);

        ErrorCondition different = new ErrorCondition();
        different.setCondition(Symbol.getSymbol(symbolValue));
        different.setDescription("other");

        assertErrorConditionsNotEqual(same1, different);
    }

    @Test
    public void testConditionDescriptionInfoEquality() {
        String symbolValue = "symbol";
        String descriptionValue = "description";

        ErrorCondition same1 = new ErrorCondition();
        same1.setCondition(Symbol.getSymbol(new String(symbolValue)));
        same1.setDescription(new String(descriptionValue));
        same1.setInfo(Collections.singletonMap(Symbol.getSymbol("key"), "value"));

        ErrorCondition same2 = new ErrorCondition();
        same2.setCondition(Symbol.getSymbol(new String(symbolValue)));
        same2.setDescription(new String(descriptionValue));
        same2.setInfo(Collections.singletonMap(Symbol.getSymbol("key"), "value"));

        assertErrorConditionsEqual(same1, same2);

        ErrorCondition different = new ErrorCondition();
        different.setCondition(Symbol.getSymbol(symbolValue));
        different.setDescription(new String(descriptionValue));
        different.setInfo(Collections.singletonMap(Symbol.getSymbol("other"), "value"));

        assertErrorConditionsNotEqual(same1, different);
    }

    private void assertErrorConditionsNotEqual(ErrorCondition error1, ErrorCondition error2) {
        assertThat(error1, is(not(error2)));
        assertThat(error2, is(not(error1)));
    }

    private void assertErrorConditionsEqual(ErrorCondition error1, ErrorCondition error2) {
        assertEquals(error1, error2);
        assertEquals(error2, error1);
        assertEquals(error1.hashCode(), error2.hashCode());
    }
}
