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
package org.apache.qpid.proton4j.test.driver.buffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class CompositeWritableBuffer implements WritableBuffer {

    private final WritableBuffer first;
    private final WritableBuffer second;

    public CompositeWritableBuffer(WritableBuffer first, WritableBuffer second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public void put(byte b) {
        (first.hasRemaining() ? first : second).put(b);
    }

    @Override
    public void putFloat(float f) {
        putInt(Float.floatToRawIntBits(f));
    }

    @Override
    public void putDouble(double d) {
        putLong(Double.doubleToRawLongBits(d));
    }

    @Override
    public void putShort(short s) {
        int remaining = first.remaining();

        if (remaining >= 2) {
            first.putShort(s);
        } else if (remaining == 0) {
            second.putShort(s);
        } else {
            ByteBuffer wrap = ByteBuffer.wrap(new byte[2]);
            wrap.putShort(s);
            wrap.flip();
            put(wrap);
        }
    }

    @Override
    public void putInt(int i) {
        int remaining = first.remaining();
        if (remaining >= 4) {
            first.putInt(i);
        } else if (remaining == 0) {
            second.putInt(i);
        } else {
            ByteBuffer wrap = ByteBuffer.wrap(new byte[4]);
            wrap.putInt(i);
            wrap.flip();
            put(wrap);
        }
    }

    @Override
    public void putLong(long l) {
        int remaining = first.remaining();

        if (remaining >= 8) {
            first.putLong(l);
        } else if (remaining == 0) {
            second.putLong(l);
        } else {
            ByteBuffer wrap = ByteBuffer.wrap(new byte[8]);
            wrap.putLong(l);
            wrap.flip();
            put(wrap);
        }
    }

    @Override
    public boolean hasRemaining() {
        return first.hasRemaining() || second.hasRemaining();
    }

    @Override
    public int remaining() {
        return first.remaining() + second.remaining();
    }

    @Override
    public int position() {
        return first.position() + second.position();
    }

    @Override
    public int limit() {
        return first.limit() + second.limit();
    }

    @Override
    public void position(int position) {
        int first_limit = first.limit();

        if (position <= first_limit) {
            first.position(position);
            second.position(0);
        } else {
            first.position(first_limit);
            second.position(position - first_limit);
        }
    }

    @Override
    public void put(byte[] src, int offset, int length) {
        final int firstRemaining = first.remaining();

        if (firstRemaining > 0) {
            if (firstRemaining >= length) {
                first.put(src, offset, length);
                return;
            } else {
                first.put(src, offset, firstRemaining);
            }
        }
        second.put(src, offset + firstRemaining, length - firstRemaining);
    }

    @Override
    public void put(ByteBuffer payload) {
        int firstRemaining = first.remaining();

        if (firstRemaining > 0) {
            if (firstRemaining >= payload.remaining()) {
                first.put(payload);
                return;
            } else {
                int limit = payload.limit();
                payload.limit(payload.position() + firstRemaining);
                first.put(payload);
                payload.limit(limit);
            }
        }

        second.put(payload);
    }

    @Override
    public String toString() {
        return first.toString() + " + " + second.toString();
    }

    @Override
    public void put(ReadableBuffer payload) {
        int firstRemaining = first.remaining();

        if (firstRemaining > 0) {
            if (firstRemaining >= payload.remaining()) {
                first.put(payload);
                return;
            } else {
                int limit = payload.limit();
                payload.limit(payload.position() + firstRemaining);
                first.put(payload);
                payload.limit(limit);
            }
        }

        second.put(payload);
    }

    @Override
    public void put(String value) {
        if (first.hasRemaining()) {
            byte[] utf8Bytes = value.getBytes(StandardCharsets.UTF_8);
            put(utf8Bytes, 0, utf8Bytes.length);
        } else {
            second.put(value);
        }
    }
}
