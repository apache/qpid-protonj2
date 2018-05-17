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
package org.apache.qpid.proton4j.transport.impl;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.apache.qpid.proton4j.transport.TransportHandler;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProtonTransportPipelineTest {

    @Mock private ProtonTransport transport;

    @Test
    public void testCreatePipeline() {
        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        assertSame(pipeline.getTransport(), transport);
        assertNull(pipeline.first());
        assertNull(pipeline.last());
        assertNull(pipeline.firstContext());
        assertNull(pipeline.lastContext());
    }

    @Test
    public void testCreatePipelineRejectsNullParent() {
        try {
            new ProtonTransportPipeline(null);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    //----- Tests for addFirst -----------------------------------------------//

    @Test
    public void testAddFirstRejectsNullHandler() {
        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        try {
            pipeline.addFirst("one", null);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddFirstRejectsNullHandlerName() {
        TransportHandler handler = Mockito.mock(TransportHandler.class);
        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        try {
            pipeline.addFirst(null, handler);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddFirstRejectsEmptyHandlerName() {
        TransportHandler handler = Mockito.mock(TransportHandler.class);
        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        try {
            pipeline.addFirst("", handler);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddFirstWithOneHandler() {
        TransportHandler handler = Mockito.mock(TransportHandler.class);

        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        pipeline.addFirst("one", handler);

        assertSame(handler, pipeline.first());
        assertSame(handler, pipeline.last());
        assertNotNull(pipeline.firstContext());
        assertSame(handler, pipeline.firstContext().getHandler());
    }

    @Test
    public void testAddFirstWithMoreThanOneHandler() {
        TransportHandler handler1 = Mockito.mock(TransportHandler.class);
        TransportHandler handler2 = Mockito.mock(TransportHandler.class);
        TransportHandler handler3 = Mockito.mock(TransportHandler.class);

        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        pipeline.addFirst("three", handler3);
        pipeline.addFirst("two", handler2);
        pipeline.addFirst("one", handler1);

        assertSame(handler1, pipeline.first());
        assertSame(handler3, pipeline.last());

        assertNotNull(pipeline.firstContext());
        assertSame(handler1, pipeline.firstContext().getHandler());
        assertNotNull(pipeline.lastContext());
        assertSame(handler3, pipeline.lastContext().getHandler());
    }

    //----- Tests for addLast ------------------------------------------------//

    @Test
    public void testAddLastRejectsNullHandler() {
        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        try {
            pipeline.addLast("one", null);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddLastRejectsNullHandlerName() {
        TransportHandler handler = Mockito.mock(TransportHandler.class);
        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        try {
            pipeline.addLast(null, handler);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddLastRejectsEmptyHandlerName() {
        TransportHandler handler = Mockito.mock(TransportHandler.class);
        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        try {
            pipeline.addLast("", handler);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddLastWithOneHandler() {
        TransportHandler handler = Mockito.mock(TransportHandler.class);

        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        pipeline.addLast("one", handler);

        assertSame(handler, pipeline.first());
        assertSame(handler, pipeline.last());

        assertNotNull(pipeline.firstContext());
        assertSame(handler, pipeline.firstContext().getHandler());
        assertNotNull(pipeline.lastContext());
        assertSame(handler, pipeline.lastContext().getHandler());
    }

    @Test
    public void testAddLastWithMoreThanOneHandler() {
        TransportHandler handler1 = Mockito.mock(TransportHandler.class);
        TransportHandler handler2 = Mockito.mock(TransportHandler.class);
        TransportHandler handler3 = Mockito.mock(TransportHandler.class);

        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        pipeline.addLast("one", handler1);
        pipeline.addLast("two", handler2);
        pipeline.addLast("three", handler3);

        assertSame(handler1, pipeline.first());
        assertSame(handler3, pipeline.last());

        assertNotNull(pipeline.firstContext());
        assertSame(handler1, pipeline.firstContext().getHandler());
        assertNotNull(pipeline.lastContext());
        assertSame(handler3, pipeline.lastContext().getHandler());
    }

    //----- Tests for removeFirst --------------------------------------------//

    @Test
    public void testRemoveFirstWithOneHandler() {
        TransportHandler handler = Mockito.mock(TransportHandler.class);

        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        pipeline.addFirst("one", handler);

        assertNotNull(pipeline.first());
        assertSame(pipeline, pipeline.removeFirst());
        assertNull(pipeline.first());
        // calling when empty should not throw.
        assertSame(pipeline, pipeline.removeFirst());
    }

    @Test
    public void testRemoveFirstWithMoreThanOneHandler() {
        TransportHandler handler1 = Mockito.mock(TransportHandler.class);
        TransportHandler handler2 = Mockito.mock(TransportHandler.class);
        TransportHandler handler3 = Mockito.mock(TransportHandler.class);

        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        pipeline.addFirst("three", handler3);
        pipeline.addFirst("two", handler2);
        pipeline.addFirst("one", handler1);

        assertSame(pipeline, pipeline.removeFirst());
        assertSame(handler2, pipeline.first());
        assertSame(pipeline, pipeline.removeFirst());
        assertSame(handler3, pipeline.first());
        assertSame(pipeline, pipeline.removeFirst());
        // calling when empty should not throw.
        assertSame(pipeline, pipeline.removeFirst());
        assertNull(pipeline.first());
    }

    //----- Tests for removeLast ---------------------------------------------//

    @Test
    public void testRemoveLastWithOneHandler() {
        TransportHandler handler = Mockito.mock(TransportHandler.class);

        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        pipeline.addFirst("one", handler);

        assertNotNull(pipeline.first());
        assertSame(pipeline, pipeline.removeLast());
        assertNull(pipeline.first());
        // calling when empty should not throw.
        assertSame(pipeline, pipeline.removeLast());
    }

    @Test
    public void testRemoveLastWithMoreThanOneHandler() {
        TransportHandler handler1 = Mockito.mock(TransportHandler.class);
        TransportHandler handler2 = Mockito.mock(TransportHandler.class);
        TransportHandler handler3 = Mockito.mock(TransportHandler.class);

        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        pipeline.addFirst("three", handler3);
        pipeline.addFirst("two", handler2);
        pipeline.addFirst("one", handler1);

        assertSame(pipeline, pipeline.removeLast());
        assertSame(handler2, pipeline.last());
        assertSame(pipeline, pipeline.removeLast());
        assertSame(handler1, pipeline.last());
        assertSame(pipeline, pipeline.removeLast());
        // calling when empty should not throw.
        assertSame(pipeline, pipeline.removeLast());
        assertNull(pipeline.last());
    }

    //----- Tests for removeLast ---------------------------------------------//

    @Test
    public void testRemoveWhenEmpty() {
        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        assertSame(pipeline, pipeline.remove("unknown"));
        assertSame(pipeline, pipeline.remove(""));
        assertSame(pipeline, pipeline.remove(null));
    }

    @Test
    public void testRemoveWithOneHandler() {
        TransportHandler handler = Mockito.mock(TransportHandler.class);

        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        pipeline.addFirst("one", handler);

        assertSame(handler, pipeline.first());

        assertSame(pipeline, pipeline.remove("unknown"));
        assertSame(pipeline, pipeline.remove(""));
        assertSame(pipeline, pipeline.remove(null));

        assertSame(handler, pipeline.first());
        assertSame(pipeline, pipeline.remove("one"));

        assertNull(pipeline.first());
        assertNull(pipeline.last());
    }

    @Test
    public void testRemoveWithMoreThanOneHandler() {
        TransportHandler handler1 = Mockito.mock(TransportHandler.class);
        TransportHandler handler2 = Mockito.mock(TransportHandler.class);
        TransportHandler handler3 = Mockito.mock(TransportHandler.class);

        ProtonTransportPipeline pipeline = new ProtonTransportPipeline(transport);

        pipeline.addFirst("three", handler3);
        pipeline.addFirst("two", handler2);
        pipeline.addFirst("one", handler1);

        assertSame(handler1, pipeline.first());
        assertSame(pipeline, pipeline.remove("one"));
        assertSame(handler2, pipeline.first());
        assertSame(pipeline, pipeline.remove("two"));
        assertSame(handler3, pipeline.first());
        assertSame(pipeline, pipeline.remove("three"));

        assertNull(pipeline.first());
        assertNull(pipeline.last());
    }
}
