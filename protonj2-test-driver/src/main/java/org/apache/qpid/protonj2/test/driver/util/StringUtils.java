/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.protonj2.test.driver.util;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;

/**
 * Set of {@link String} utilities used in the proton code.
 */
public class StringUtils {

    private static final int DEFAULT_QUOTED_STRING_LIMIT = 64;

    /**
     * Converts the given String[] into a Symbol[] array.
     *
     * @param stringArray
     * 		The given String[] to convert.
     *
     * @return a new Symbol array that contains Symbol versions of the input Strings.
     */
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

    /**
     * Converts the given Symbol[] into a String[] array.
     *
     * @param symbolArray
     * 		The given Symbol[] to convert.
     *
     * @return a new String array that contains String versions of the input Symbol.
     */
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

    /**
     * Converts the given String keyed {@link Map} into a matching Symbol keyed {@link Map}.
     *
     * @param stringsMap
     * 		The given String keyed {@link Map} to convert.
     *
     * @return a new Symbol keyed {@link Map} that contains Symbol versions of the input String keys.
     */
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
     * Converts the given Symbol keyed {@link Map} into a matching String keyed {@link Map}.
     *
     * @param symbolMap
     * 		The given String keyed {@link Map} to convert.
     *
     * @return a new String keyed {@link Map} that contains String versions of the input Symbol keys.
     */
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

    /**
     * Converts the given String {@link Collection} into a Symbol array.
     *
     * @param stringsSet
     * 		The given String {@link Collection} to convert.
     *
     * @return a new Symbol array that contains String versions of the input Symbols.
     */
    public static Symbol[] toSymbolArray(Collection<String> stringsSet) {
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

    /**
     * Converts the given String {@link Collection} into a matching Symbol {@link Set}.
     *
     * @param stringsSet
     * 		The given String {@link Collection} to convert.
     *
     * @return a new Symbol {@link Set} that contains String versions of the input Symbols.
     */
    public static Set<Symbol> toSymbolSet(Collection<String> stringsSet) {
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

    /**
     * Converts the given Symbol array into a matching String {@link Set}.
     *
     * @param symbols
     * 		The given Symbol array to convert.
     *
     * @return a new String {@link Set} that contains String versions of the input Symbols.
     */
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
        if (buffer == null) {
            return "\"\"";
        } else {
            return toQuotedString(buffer.asByteBuffer(), stringLength, appendIfTruncated);
        }
    }

    /**
     * Converts the ProtonBuffer to a quoted string using a default max length before truncation value and
     * appends a truncation indication if the string required truncation.
     *
     * @param buffer
     *        the {@link ByteBuffer} to convert into String format.
     *
     * @return the converted string
     */
    public static String toQuotedString(final ByteBuffer buffer) {
        return toQuotedString(buffer, DEFAULT_QUOTED_STRING_LIMIT, true);
    }

    /**
     * Converts the ProtonBuffer to a quoted string using a default max length before truncation value.
     *
     * @param buffer
     *        the {@link ByteBuffer} to convert into String format.
     * @param appendIfTruncated
     *        appends "...(truncated)" if not all of the payload is present in the string
     *
     * @return the converted string
     */
    public static String toQuotedString(final ByteBuffer buffer, final boolean appendIfTruncated) {
        return toQuotedString(buffer, DEFAULT_QUOTED_STRING_LIMIT, appendIfTruncated);
    }

    /**
     * Converts the ProtonBuffer to a quoted string.
     *
     * @param buffer
     *        the {@link ByteBuffer} to convert into String format.
     * @param stringLength
     *        the maximum length of stringified content (excluding the quotes, and truncated indicator)
     * @param appendIfTruncated
     *        appends "...(truncated)" if not all of the payload is present in the string
     *
     * @return the converted string
     */
    public static String toQuotedString(final ByteBuffer buffer, final int stringLength, final boolean appendIfTruncated) {
        if (buffer == null) {
            return "\"\"";
        }

        StringBuilder str = new StringBuilder();
        str.append("\"");

        final int byteToRead = buffer.remaining();
        int size = 0;
        boolean truncated = false;

        for (int i = 0; i < byteToRead; ++i) {
            byte c = buffer.get(i);

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
