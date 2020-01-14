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
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.messaging.Modified;
import org.apache.qpid.proton4j.amqp.messaging.Rejected;
import org.apache.qpid.proton4j.amqp.messaging.Released;
import org.messaginghub.amqperative.DeliveryState;

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
                result[i] = symbolArray[i].toString();
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

    public static Symbol[] toSymbolArray(Set<String> stringsSet) {
        final Symbol[] result;

        if (stringsSet != null) {
            result = new Symbol[stringsSet.size()];
            int index = 0;
            for (String entry : stringsSet) {
                result[index++] = Symbol.valueOf(entry);
            }
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

    public static Set<String> toStringSet(Symbol[] symbols) {
        Set<String> result;

        if (symbols != null) {
            result = new LinkedHashSet<>(symbols.length);
            for (Symbol symbol : symbols) {
                result.add(symbol.toString());
            }
        } else {
            result = null;
        }

        return result;
    }

    public static Symbol[] outcomesToSymbols(DeliveryState.Type[] outcomes) {
        Symbol[] result = null;

        if (outcomes != null) {
            result = new Symbol[outcomes.length];
            for (int i = 0; i < outcomes.length; ++i) {
                result[i] = outcomeToSymbol(outcomes[i]);
            }
        }

        return result;
    }

    public static DeliveryState.Type[] symbolsToOutcomes(Symbol[] outcomes) {
        DeliveryState.Type[] result = null;

        if (outcomes != null) {
            result = new DeliveryState.Type[outcomes.length];
            for (int i = 0; i < outcomes.length; ++i) {
                result[i] = symbolToOutcome(outcomes[i]);
            }
        }

        return result;
    }

    public static Symbol outcomeToSymbol(DeliveryState.Type outcome) {
        if (outcome == null) {
            return null;
        }

        switch (outcome) {
            case ACCEPTED:
                return Accepted.DESCRIPTOR_SYMBOL;
            case REJECTED:
                return Rejected.DESCRIPTOR_SYMBOL;
            case RELEASED:
                return Released.DESCRIPTOR_SYMBOL;
            case MODIFIED:
                return Modified.DESCRIPTOR_SYMBOL;
            default:
                throw new IllegalArgumentException("DeliveryState.Type " + outcome + " cannot be applied as an outcome");
        }
    }

    public static DeliveryState.Type symbolToOutcome(Symbol outcome) {
        if (outcome == null) {
            return null;
        } else if (outcome.equals(Accepted.DESCRIPTOR_SYMBOL)) {
            return DeliveryState.Type.ACCEPTED;
        } else if (outcome.equals(Rejected.DESCRIPTOR_SYMBOL)) {
            return DeliveryState.Type.REJECTED;
        } else if (outcome.equals(Released.DESCRIPTOR_SYMBOL)) {
            return DeliveryState.Type.RELEASED;
        } else if (outcome.equals(Modified.DESCRIPTOR_SYMBOL)) {
            return DeliveryState.Type.MODIFIED;
        } else {
            throw new IllegalArgumentException("Cannot convert Symbol: " + outcome + " to a DeliveryState.Type outcome");
        }
    }
}