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
package org.apache.qpid.proton4j.engine.impl;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.apache.qpid.proton4j.engine.EngineHandler;
import org.apache.qpid.proton4j.engine.impl.ProtonEngine;
import org.apache.qpid.proton4j.engine.impl.ProtonEnginePipeline;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProtonTransportPipelineTest {

    @Mock private ProtonEngine transport;

    @Test
    public void testCreatePipeline() {
        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

        assertSame(pipeline.getTransport(), transport);
        assertNull(pipeline.first());
        assertNull(pipeline.last());
        assertNull(pipeline.firstContext());
        assertNull(pipeline.lastContext());
    }

    @Test
    public void testCreatePipelineRejectsNullParent() {
        try {
            new ProtonEnginePipeline(null);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    //----- Tests for addFirst -----------------------------------------------//

    @Test
    public void testAddFirstRejectsNullHandler() {
        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

        try {
            pipeline.addFirst("one", null);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddFirstRejectsNullHandlerName() {
        EngineHandler handler = Mockito.mock(EngineHandler.class);
        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

        try {
            pipeline.addFirst(null, handler);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddFirstRejectsEmptyHandlerName() {
        EngineHandler handler = Mockito.mock(EngineHandler.class);
        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

        try {
            pipeline.addFirst("", handler);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddFirstWithOneHandler() {
        EngineHandler handler = Mockito.mock(EngineHandler.class);

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

        pipeline.addFirst("one", handler);

        assertSame(handler, pipeline.first());
        assertSame(handler, pipeline.last());
        assertNotNull(pipeline.firstContext());
        assertSame(handler, pipeline.firstContext().getHandler());
    }

    @Test
    public void testAddFirstWithMoreThanOneHandler() {
        EngineHandler handler1 = Mockito.mock(EngineHandler.class);
        EngineHandler handler2 = Mockito.mock(EngineHandler.class);
        EngineHandler handler3 = Mockito.mock(EngineHandler.class);

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

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
        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

        try {
            pipeline.addLast("one", null);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddLastRejectsNullHandlerName() {
        EngineHandler handler = Mockito.mock(EngineHandler.class);
        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

        try {
            pipeline.addLast(null, handler);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddLastRejectsEmptyHandlerName() {
        EngineHandler handler = Mockito.mock(EngineHandler.class);
        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

        try {
            pipeline.addLast("", handler);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddLastWithOneHandler() {
        EngineHandler handler = Mockito.mock(EngineHandler.class);

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

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
        EngineHandler handler1 = Mockito.mock(EngineHandler.class);
        EngineHandler handler2 = Mockito.mock(EngineHandler.class);
        EngineHandler handler3 = Mockito.mock(EngineHandler.class);

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

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
        EngineHandler handler = Mockito.mock(EngineHandler.class);

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

        pipeline.addFirst("one", handler);

        assertNotNull(pipeline.first());
        assertSame(pipeline, pipeline.removeFirst());
        assertNull(pipeline.first());
        // calling when empty should not throw.
        assertSame(pipeline, pipeline.removeFirst());
    }

    @Test
    public void testRemoveFirstWithMoreThanOneHandler() {
        EngineHandler handler1 = Mockito.mock(EngineHandler.class);
        EngineHandler handler2 = Mockito.mock(EngineHandler.class);
        EngineHandler handler3 = Mockito.mock(EngineHandler.class);

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

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
        EngineHandler handler = Mockito.mock(EngineHandler.class);

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

        pipeline.addFirst("one", handler);

        assertNotNull(pipeline.first());
        assertSame(pipeline, pipeline.removeLast());
        assertNull(pipeline.first());
        // calling when empty should not throw.
        assertSame(pipeline, pipeline.removeLast());
    }

    @Test
    public void testRemoveLastWithMoreThanOneHandler() {
        EngineHandler handler1 = Mockito.mock(EngineHandler.class);
        EngineHandler handler2 = Mockito.mock(EngineHandler.class);
        EngineHandler handler3 = Mockito.mock(EngineHandler.class);

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

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
        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

        assertSame(pipeline, pipeline.remove("unknown"));
        assertSame(pipeline, pipeline.remove(""));
        assertSame(pipeline, pipeline.remove(null));
    }

    @Test
    public void testRemoveWithOneHandler() {
        EngineHandler handler = Mockito.mock(EngineHandler.class);

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

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
        EngineHandler handler1 = Mockito.mock(EngineHandler.class);
        EngineHandler handler2 = Mockito.mock(EngineHandler.class);
        EngineHandler handler3 = Mockito.mock(EngineHandler.class);

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(transport);

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
