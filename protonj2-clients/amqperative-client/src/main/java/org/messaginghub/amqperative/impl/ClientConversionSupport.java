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
package org.messaginghub.amqperative.impl;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.proton4j.amqp.Symbol;

/**
 * Utilities used by various classes in the Client core
 */
abstract class ClientConversionSupport {

    public static Symbol[] toSymbolArray(String[] stringArray) {
        Symbol[] result = null;

        if (stringArray != null) {
            result = new Symbol[stringArray.length];
            for (int i = 0; i < stringArray.length; ++i) {
                result[i] = Symbol.valueOf(stringArray[i]);
            }
        }

        return result;
    }

    public static String[] toStringArray(Symbol[] symbolArray) {
        String[] result = null;

        if (symbolArray != null) {
            result = new String[symbolArray.length];
            for (int i = 0; i < symbolArray.length; ++i) {
                result[i] = symbolArray.toString();
            }
        }

        return result;
    }

    public static Map<Symbol, Object> toSymbolKeyedMap(Map<String, Object> stringsMap) {
        final Map<Symbol, Object> result;

        if (stringsMap != null) {
            result = new HashMap<>(stringsMap.size());
            stringsMap.forEach((key, value) -> {
                result.put(Symbol.valueOf(key), value);
            });
        } else {
            result = null;
        }

        return result;
    }

    public static Map<String, Object> toStringKeyedMap(Map<Symbol, Object> symbolMap) {
        Map<String, Object> result;

        if (symbolMap != null) {
            result = new LinkedHashMap<>(symbolMap.size());
            symbolMap.forEach((key, value) -> {
                result.put(key.toString(), value);
            });
        } else {
            result = null;
        }

        return result;
    }

    public static Set<Symbol> toSymbolSet(Set<String> stringsSet) {
        final Set<Symbol> result;

        if (stringsSet != null) {
            result = new LinkedHashSet<>(stringsSet.size());
            stringsSet.forEach((entry) -> {
                result.add(Symbol.valueOf(entry));
            });
        } else {
            result = null;
        }

        return result;
    }

    public static Set<String> toStringSet(Set<Symbol> symbolSet) {
        Set<String> result;

        if (symbolSet != null) {
            result = new LinkedHashSet<>(symbolSet.size());
            symbolSet.forEach((entry) -> {
                result.add(entry.toString());
            });
        } else {
            result = null;
        }

        return result;
    }
}
