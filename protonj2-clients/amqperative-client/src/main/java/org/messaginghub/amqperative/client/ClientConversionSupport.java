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
package org.messaginghub.amqperative.client;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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
        Map<Symbol, Object> result = null;

        if (stringsMap != null) {
            Map<Symbol, Object> properties = new HashMap<>(stringsMap.size());
            for (Entry<String, Object> entry : stringsMap.entrySet()) {
                properties.put(Symbol.valueOf(entry.getKey()), entry.getValue());
            }
        }

        return result;
    }

    public static Map<String, Object> toStringKeyedMap(Map<Symbol, Object> symbokMap) {
        Map<String, Object> result = null;

        if (symbokMap != null) {
            Map<String, Object> properties = new HashMap<>(symbokMap.size());
            for (Entry<Symbol, Object> entry : symbokMap.entrySet()) {
                properties.put(entry.getKey().toString(), entry.getValue());
            }
        }

        return result;
    }
}
