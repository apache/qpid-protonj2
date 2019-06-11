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
package org.apache.qpid.proton4j;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.junit.Test;

public class SymbolTest {

    @Test
    public void testCompareTo() {
        String symbolString1 = "Symbol-1";
        String symbolString2 = "Symbol-2";
        String symbolString3 = "Symbol-3";

        Symbol symbol1 = Symbol.valueOf(symbolString1);
        Symbol symbol2 = Symbol.valueOf(symbolString2);
        Symbol symbol3 = Symbol.valueOf(symbolString3);

        assertEquals(0, symbol1.compareTo(symbol1));
        assertEquals(0, symbol2.compareTo(symbol2));
        assertEquals(0, symbol3.compareTo(symbol3));

        assertTrue(symbol2.compareTo(symbol1) > 0);
        assertTrue(symbol3.compareTo(symbol1) > 0);
        assertTrue(symbol3.compareTo(symbol2) > 0);

        assertTrue(symbol1.compareTo(symbol2) < 0);
        assertTrue(symbol1.compareTo(symbol3) < 0);
        assertTrue(symbol2.compareTo(symbol3) < 0);
    }

    @Test
    public void testEquals() {
        String symbolString1 = "Symbol-1";
        String symbolString2 = "Symbol-2";
        String symbolString3 = "Symbol-3";

        Symbol symbol1 = Symbol.valueOf(symbolString1);
        Symbol symbol2 = Symbol.valueOf(symbolString2);
        Symbol symbol3 = Symbol.valueOf(symbolString3);

        assertNotEquals(symbol1, symbol2);

        assertEquals(symbolString1, symbol1.toString());
        assertEquals(symbolString2, symbol2.toString());
        assertEquals(symbolString3, symbol3.toString());

        assertNotEquals(symbol1, symbol2);
        assertNotEquals(symbol2, symbol3);
        assertNotEquals(symbol3, symbol1);

        assertNotEquals(symbolString1, symbol1);
        assertNotEquals(symbolString2, symbol2);
        assertNotEquals(symbolString3, symbol3);
    }

    @Test
    public void testHashcode() {
        String symbolString1 = "Symbol-1";
        String symbolString2 = "Symbol-2";

        Symbol symbol1 = Symbol.valueOf(symbolString1);
        Symbol symbol2 = Symbol.valueOf(symbolString2);

        assertNotEquals(symbol1, symbol2);
        assertNotEquals(symbol1.hashCode(), symbol2.hashCode());

        assertEquals(symbol1.hashCode(), Symbol.valueOf(symbolString1).hashCode());
        assertEquals(symbol2.hashCode(), Symbol.valueOf(symbolString2).hashCode());
    }

    @Test
    public void testValueOf() {
        String symbolString1 = "Symbol-1";
        String symbolString2 = "Symbol-2";

        Symbol symbol1 = Symbol.valueOf(symbolString1);
        Symbol symbol2 = Symbol.valueOf(symbolString2);

        assertNotEquals(symbol1, symbol2);

        assertEquals(symbolString1, symbol1.toString());
        assertEquals(symbolString2, symbol2.toString());
    }

    @Test
    public void testValueOfProducesSingleton() {
        String symbolString = "Symbol-String";

        Symbol symbol1 = Symbol.valueOf(symbolString);
        Symbol symbol2 = Symbol.valueOf(symbolString);

        assertEquals(symbolString, symbol1.toString());
        assertEquals(symbolString, symbol2.toString());

        assertSame(symbol1, symbol2);
    }

    @Test
    public void testGetSymbol() {
        String symbolString1 = "Symbol-1";
        String symbolString2 = "Symbol-2";

        Symbol symbol1 = Symbol.getSymbol(symbolString1);
        Symbol symbol2 = Symbol.getSymbol(symbolString2);

        assertNotEquals(symbol1, symbol2);

        assertEquals(symbolString1, symbol1.toString());
        assertEquals(symbolString2, symbol2.toString());
    }

    @Test
    public void testGetSymbolProducesSingleton() {
        String symbolString = "Symbol-String";

        Symbol symbol1 = Symbol.getSymbol(symbolString);
        Symbol symbol2 = Symbol.getSymbol(symbolString);

        assertEquals(symbolString, symbol1.toString());
        assertEquals(symbolString, symbol2.toString());

        assertSame(symbol1, symbol2);
    }

    @Test
    public void testGetSymbolAndValueOfProduceSingleton() {
        String symbolString = "Symbol-String";

        Symbol symbol1 = Symbol.valueOf(symbolString);
        Symbol symbol2 = Symbol.getSymbol(symbolString);

        assertEquals(symbolString, symbol1.toString());
        assertEquals(symbolString, symbol2.toString());

        assertSame(symbol1, symbol2);
    }
}
