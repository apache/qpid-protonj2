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
package org.apache.qpid.protonj2.engine.impl;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.qpid.protonj2.engine.EngineHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ProtonEnginePipelineTest {

    private ProtonEngine engine;

    @BeforeEach
    public void initMocks() {
        engine = Mockito.mock(ProtonEngine.class);
    }

    @Test
    public void testCreatePipeline() {
        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

        assertSame(pipeline.engine(), engine);
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
        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

        try {
            pipeline.addFirst("one", null);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddFirstRejectsNullHandlerName() {
        EngineHandler handler = Mockito.mock(EngineHandler.class);
        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

        try {
            pipeline.addFirst(null, handler);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddFirstRejectsEmptyHandlerName() {
        EngineHandler handler = Mockito.mock(EngineHandler.class);
        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

        try {
            pipeline.addFirst("", handler);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddFirstWithOneHandler() {
        EngineHandler handler = Mockito.mock(EngineHandler.class);

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

        pipeline.addFirst("one", handler);

        assertSame(handler, pipeline.first());
        assertSame(handler, pipeline.last());
        assertNotNull(pipeline.firstContext());
        assertSame(handler, pipeline.firstContext().handler());
    }

    @Test
    public void testAddFirstWithMoreThanOneHandler() {
        EngineHandler handler1 = Mockito.mock(EngineHandler.class);
        EngineHandler handler2 = Mockito.mock(EngineHandler.class);
        EngineHandler handler3 = Mockito.mock(EngineHandler.class);

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

        pipeline.addFirst("three", handler3);
        pipeline.addFirst("two", handler2);
        pipeline.addFirst("one", handler1);

        assertSame(handler1, pipeline.first());
        assertSame(handler3, pipeline.last());

        assertNotNull(pipeline.firstContext());
        assertSame(handler1, pipeline.firstContext().handler());
        assertNotNull(pipeline.lastContext());
        assertSame(handler3, pipeline.lastContext().handler());
    }

    //----- Tests for addLast ------------------------------------------------//

    @Test
    public void testAddLastRejectsNullHandler() {
        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

        try {
            pipeline.addLast("one", null);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddLastRejectsNullHandlerName() {
        EngineHandler handler = Mockito.mock(EngineHandler.class);
        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

        try {
            pipeline.addLast(null, handler);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddLastRejectsEmptyHandlerName() {
        EngineHandler handler = Mockito.mock(EngineHandler.class);
        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

        try {
            pipeline.addLast("", handler);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddLastWithOneHandler() {
        EngineHandler handler = Mockito.mock(EngineHandler.class);

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

        pipeline.addLast("one", handler);

        assertSame(handler, pipeline.first());
        assertSame(handler, pipeline.last());

        assertNotNull(pipeline.firstContext());
        assertSame(handler, pipeline.firstContext().handler());
        assertNotNull(pipeline.lastContext());
        assertSame(handler, pipeline.lastContext().handler());
    }

    @Test
    public void testAddLastWithMoreThanOneHandler() {
        EngineHandler handler1 = Mockito.mock(EngineHandler.class);
        EngineHandler handler2 = Mockito.mock(EngineHandler.class);
        EngineHandler handler3 = Mockito.mock(EngineHandler.class);

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

        pipeline.addLast("one", handler1);
        pipeline.addLast("two", handler2);
        pipeline.addLast("three", handler3);

        assertSame(handler1, pipeline.first());
        assertSame(handler3, pipeline.last());

        assertNotNull(pipeline.firstContext());
        assertSame(handler1, pipeline.firstContext().handler());
        assertNotNull(pipeline.lastContext());
        assertSame(handler3, pipeline.lastContext().handler());
    }

    //----- Tests for removeFirst --------------------------------------------//

    @Test
    public void testRemoveFirstWithOneHandler() {
        EngineHandler handler = Mockito.mock(EngineHandler.class);

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

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

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

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

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

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

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

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
        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

        assertSame(pipeline, pipeline.remove("unknown"));
        assertSame(pipeline, pipeline.remove(""));
        assertSame(pipeline, pipeline.remove((String) null));
        assertSame(pipeline, pipeline.remove((EngineHandler) null));
    }

    @Test
    public void testRemoveWithOneHandler() {
        EngineHandler handler = Mockito.mock(EngineHandler.class);

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

        pipeline.addFirst("one", handler);

        assertSame(handler, pipeline.first());

        assertSame(pipeline, pipeline.remove("unknown"));
        assertSame(pipeline, pipeline.remove(""));
        assertSame(pipeline, pipeline.remove((String) null));
        assertSame(pipeline, pipeline.remove((EngineHandler) null));

        assertSame(handler, pipeline.first());
        assertSame(pipeline, pipeline.remove("one"));

        assertNull(pipeline.first());
        assertNull(pipeline.last());

        pipeline.addFirst("one", handler);

        assertSame(handler, pipeline.first());

        assertSame(pipeline, pipeline.remove(handler));

        assertNull(pipeline.first());
        assertNull(pipeline.last());
    }

    @Test
    public void testRemoveWithMoreThanOneHandler() {
        EngineHandler handler1 = Mockito.mock(EngineHandler.class);
        EngineHandler handler2 = Mockito.mock(EngineHandler.class);
        EngineHandler handler3 = Mockito.mock(EngineHandler.class);

        ProtonEnginePipeline pipeline = new ProtonEnginePipeline(engine);

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

        pipeline.addFirst("three", handler3);
        pipeline.addFirst("two", handler2);
        pipeline.addFirst("one", handler1);

        assertSame(handler1, pipeline.first());
        assertSame(pipeline, pipeline.remove(handler1));
        assertSame(handler2, pipeline.first());
        assertSame(pipeline, pipeline.remove(handler2));
        assertSame(handler3, pipeline.first());
        assertSame(pipeline, pipeline.remove(handler3));

        assertNull(pipeline.first());
        assertNull(pipeline.last());
    }
}
