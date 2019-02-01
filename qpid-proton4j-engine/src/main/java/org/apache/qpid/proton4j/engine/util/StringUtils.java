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
package org.apache.qpid.proton4j.engine.util;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;

public class StringUtils {

    /**
     * Converts the Binary to a quoted string.
     *
     * @param bin
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
        }

        ProtonBuffer wrapped = ProtonByteBufferAllocator.DEFAULT.wrap(buffer.getArray(), buffer.getArrayOffset(), buffer.getLength());

        return toQuotedString(wrapped, stringLength, appendIfTruncated);
    }

    /**
     * Converts the ProtonBuffer to a quoted string.
     *
     * @param bin
     *        the {@link ProtonBuffer} to convert into String format.
     * @param stringLength
     *        the maximum length of stringified content (excluding the quotes, and truncated indicator)
     * @param appendIfTruncated
     *        appends "...(truncated)" if not all of the payload is present in the string
     *
     * @return the converted string
     */
    public static String toQuotedString(final ProtonBuffer buffer, final int stringLength, final boolean appendIfTruncated) {
        if (buffer == null) {
            return "\"\"";
        }

        StringBuilder str = new StringBuilder();
        str.append("\"");

        final int byteToRead = buffer.getReadableBytes();
        int size = 0;
        boolean truncated = false;

        for (int i = 0; i < byteToRead; ++i) {
            byte c = buffer.getByte(i);

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
