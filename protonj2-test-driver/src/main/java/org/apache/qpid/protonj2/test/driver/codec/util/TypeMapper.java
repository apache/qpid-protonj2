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
package org.apache.qpid.protonj2.test.driver.codec.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;

public abstract class TypeMapper {

    private static final int DEFAULT_QUOTED_STRING_LIMIT = 64;

    private TypeMapper() {
    }

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

    /**
     * Converts the Binary to a quoted string using a default max length before truncation value and
     * appends a truncation indication if the string required truncation.
     *
     * @param buffer
     *        the {@link Binary} to convert into String format.
     *
     * @return the converted string
     */
    public static String toQuotedString(final Binary buffer) {
        return toQuotedString(buffer, DEFAULT_QUOTED_STRING_LIMIT, true);
    }

    /**
     * Converts the Binary to a quoted string using a default max length before truncation value.
     *
     * @param buffer
     *        the {@link Binary} to convert into String format.
     * @param appendIfTruncated
     *        appends "...(truncated)" if not all of the payload is present in the string
     *
     * @return the converted string
     */
    public static String toQuotedString(final Binary buffer, final boolean appendIfTruncated) {
        return toQuotedString(buffer, DEFAULT_QUOTED_STRING_LIMIT, appendIfTruncated);
    }

    /**
     * Converts the Binary to a quoted string.
     *
     * @param buffer
     *        the {@link Binary} to convert into String format.
     * @param stringLength
     *        the maximum length of stringified content (excluding the quotes, and truncated indicator)
     * @param appendIfTruncated
     *        appends "...(truncated)" if not all of the payload is present in the string
     *
     * @return the converted string
     */
    public static String toQuotedString(final Binary buffer, final int stringLength, final boolean appendIfTruncated) {
        if (buffer == null || buffer.getArray() == null) {
            return "\"\"";
        }

        StringBuilder str = new StringBuilder();
        str.append("\"");

        final int byteToRead = buffer.getLength();
        int size = 0;
        boolean truncated = false;

        for (int i = 0; i < byteToRead; ++i) {
            byte c = buffer.getArray()[i];

            if (c > 31 && c < 127 && c != '\\') {
                if (size + 1 <= stringLength) {
                    size += 1;
                    str.append((char) c);
                } else {
                    truncated = true;
                    break;
                }
            } else {
                if (size + 4 <= stringLength) {
                    size += 4;
                    str.append(String.format("\\x%02x", c));
                } else {
                    truncated = true;
                    break;
                }
            }
        }

        str.append("\"");

        if (truncated && appendIfTruncated) {
            str.append("...(truncated)");
        }

        return str.toString();
    }
}
