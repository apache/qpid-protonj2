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
package org.apache.qpid.protonj2.client.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeoutException;

import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIOException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.junit.jupiter.api.Test;

class ClientExceptionSupportTest {

    @Test
    void testClientIOExceptionPassesThrough() {
        ClientIOException ioError = new ClientIOException("Fatal IO Error");

        assertSame(ioError, ClientExceptionSupport.createOrPassthroughFatal(ioError));
    }

    @Test
    void testClientExceptionNotPassesThrough() {
        ClientException error = new ClientException("Fatal IO Error");

        assertNotSame(error, ClientExceptionSupport.createOrPassthroughFatal(error));
        assertTrue(ClientExceptionSupport.createOrPassthroughFatal(error) instanceof ClientIOException);
    }

    @Test
    void testErrorMessageTakenFromToStringIfNotPresentInException() {
        Throwable error = new Exception() {

            private static final long serialVersionUID = 1L;

            @Override
            public String toString() {
                return "expected";
            }
        };

        assertNotSame(error, ClientExceptionSupport.createOrPassthroughFatal(error));
        assertEquals("expected", ClientExceptionSupport.createOrPassthroughFatal(error).getMessage());
    }

    @Test
    void testErrorMessageTakenFromToStringIfEmptyInException() {
        Throwable error = new Exception("") {

            private static final long serialVersionUID = 1L;

            @Override
            public String toString() {
                return "expected";
            }
        };

        assertNotSame(error, ClientExceptionSupport.createOrPassthroughFatal(error));
        assertEquals("expected", ClientExceptionSupport.createOrPassthroughFatal(error).getMessage());
    }

    @Test
    void testCauseIsClientIOExceptionExtractedAndPassedThrough() {
        ClientException error = new ClientException("Fatal IO Error", new ClientIOException("real error"));

        assertNotSame(error, ClientExceptionSupport.createOrPassthroughFatal(error));
        assertSame(error.getCause(), ClientExceptionSupport.createOrPassthroughFatal(error));
    }

    @Test
    void testClientExceptionPassesThrough() {
        ClientIOException error = new ClientIOException("Non Fatal Error");

        assertSame(error, ClientExceptionSupport.createNonFatalOrPassthrough(error));
    }

    @Test
    void testClientIOExceptionPassesThroughNonFatalCreate() {
        ClientIOException error = new ClientIOException("Fatal IO Error");

        assertSame(error, ClientExceptionSupport.createNonFatalOrPassthrough(error));
    }

    @Test
    void testErrorMessageTakenFromToStringIfNotPresentInExceptionFromNonFatalCreate() {
        Throwable error = new Exception() {

            private static final long serialVersionUID = 1L;

            @Override
            public String toString() {
                return "expected";
            }
        };

        assertNotSame(error, ClientExceptionSupport.createNonFatalOrPassthrough(error));
        assertEquals("expected", ClientExceptionSupport.createNonFatalOrPassthrough(error).getMessage());
    }

    @Test
    void testErrorMessageTakenFromToStringIfEmptyInExceptionFromNonFatalCreate() {
        Throwable error = new Exception("") {

            private static final long serialVersionUID = 1L;

            @Override
            public String toString() {
                return "expected";
            }
        };

        assertNotSame(error, ClientExceptionSupport.createNonFatalOrPassthrough(error));
        assertEquals("expected", ClientExceptionSupport.createNonFatalOrPassthrough(error).getMessage());
    }

    @Test
    void testCauseIsClientIOExceptionExtractedAndPassedThroughFromNonFatalCreate() {
        Exception error = new RuntimeException("Fatal IO Error", new ClientIOException("real error"));

        assertNotSame(error, ClientExceptionSupport.createNonFatalOrPassthrough(error));
        assertSame(error.getCause(), ClientExceptionSupport.createNonFatalOrPassthrough(error));
    }

    @Test
    void testTimeoutExceptionConvertedToClientEquivalent() {
        TimeoutException error = new TimeoutException("timeout");

        assertNotSame(error, ClientExceptionSupport.createNonFatalOrPassthrough(error));
        assertTrue(ClientExceptionSupport.createNonFatalOrPassthrough(error) instanceof ClientOperationTimedOutException);
    }

    @Test
    void testIllegalStateExceptionConvertedToClientEquivalent() {
        IllegalStateException error = new IllegalStateException("timeout");

        assertNotSame(error, ClientExceptionSupport.createNonFatalOrPassthrough(error));
        assertTrue(ClientExceptionSupport.createNonFatalOrPassthrough(error) instanceof ClientIllegalStateException);
    }
}
