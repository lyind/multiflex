/*
 * Copyright (C) 2017  Jonas Zeiger <jonas.zeiger@talpidae.net>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.talpidae.multiflex.store.sqlite;

import me.lemire.integercompression.*;
import me.lemire.integercompression.differential.Delta;
import net.talpidae.multiflex.format.Encoding;
import net.talpidae.multiflex.store.StoreException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.IntBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;


class Encoder
{
    private static ThreadLocal<Encoder> INSTANCES = ThreadLocal.withInitial(Encoder::new);

    private final SkippableComposition INT32_VAR_BYTE_FAST_PFOR;

    private final SkippableComposition INT32_DELTA_VAR_BYTE_FAST_PFOR;

    private final CharsetEncoder utf8Encoder;


    private Encoder()
    {
        INT32_VAR_BYTE_FAST_PFOR = new SkippableComposition(new HeapFastPFOR(), new HeapVariableByte());
        INT32_DELTA_VAR_BYTE_FAST_PFOR = new DeltaSkippableComposition(new HeapFastPFOR(), new HeapVariableByte());

        utf8Encoder = StandardCharsets.UTF_8.newEncoder();
    }

    /**
     * Decode a binary field.
     *
     * @param data     Buffer view of the compressed data.
     * @param encoding The encoding to use (must be compatible with outputClass).
     * @return Decoded bytes as ByteBuffer.
     */
    static ByteBuffer decodeBinary(ByteBuffer data, Encoding encoding) throws StoreException
    {
        return data.asReadOnlyBuffer();
    }

    /**
     * Decode a text field to String.
     *
     * @param data     Buffer view of the compressed data.
     * @param encoding The encoding to use (must be compatible with outputClass).
     * @return Decoded text as String.
     */
    static String decodeText(ByteBuffer data, Encoding encoding) throws StoreException
    {
        return StandardCharsets.UTF_8.decode(data).toString();
    }

    /**
     * Decode a int[] field.
     *
     * @param data     Buffer view of the compressed data.
     * @param encoding The encoding to use (must be compatible with outputClass).
     * @return Decoded text as String.
     */
    static int[] decodeIntegers(ByteBuffer data, int outLength, Encoding encoding) throws StoreException
    {
        // TODO Make this read only as much as needed when headlessUncompress supports IntBuffers
        final int inLength = data.remaining() / 4;
        final int[] in = new int[inLength];
        data.asIntBuffer().get(in);

        final int[] out = new int[outLength];
        final IntWrapper inPosition = new IntWrapper();
        final IntWrapper outPosition = new IntWrapper();
        final Encoder encoder = getInstance();

        switch (encoding)
        {
            case INT32_VAR_BYTE_FAST_PFOR:
            {
                // TODO Optimize this by making compression directly support ByteBuffer/IntBuffer as input/output or use array()
                encoder.INT32_VAR_BYTE_FAST_PFOR.headlessUncompress(in, inPosition, inLength, out, outPosition, outLength);
                break;
            }

            case INT32_DELTA_VAR_BYTE_FAST_PFOR:
            {
                // TODO Optimize this by making compression directly support ByteBuffer/IntBuffer as input/output or use array()
                encoder.INT32_DELTA_VAR_BYTE_FAST_PFOR.headlessUncompress(in, inPosition, inLength, out, outPosition, outLength);
                break;
            }

            default:
                throw new StoreException("illegal combination of encoding " + encoding.name() + " and value of type int[]");
        }

        if (outPosition.get() != outLength)
        {
            throw new StoreException("decompressed unexpected number of integers: expected " + outLength + ", got " + outPosition.get());
        }

        // consume input
        data.position(data.position() + (inPosition.get() * 4));

        return out;
    }

    /**
     * Encode a field of a binary type.
     *
     * @param in       The input buffer to encode.
     * @param out      Buffer to write the compressed data to.
     * @param encoding The encoding to use (must a compatible with class of input).
     */
    static void encodeBinary(ByteBuffer in, ByteBuffer out, Encoding encoding)
    {
        out.put(in);
    }

    /**
     * Encode a field of a text type.
     *
     * @param in       The string to encode.
     * @param out      Buffer to write the compressed data to.
     * @param encoding The encoding to use (must a compatible with class of input).
     */
    static void encodeText(String in, ByteBuffer out, Encoding encoding) throws StoreException
    {
        if (encoding == Encoding.UTF8_STRING)
        {
            // TODO Possibly optimize (directly feed String chars to encoder?)
            final CharBuffer inBuffer = CharBuffer.allocate(in.length());
            inBuffer.put(in).flip();

            Encoder.encodeText(inBuffer, out, encoding);
        }
        else
        {
            throw new StoreException("illegal combination of encoding " + encoding.name() + " and value of type String");
        }
    }

    /**
     * Encode a field of a text type.
     *
     * @param in       The CharBuffer containing the characters to encode.
     * @param out      Buffer to write the compressed data to.
     * @param encoding The encoding to use (must a compatible with class of input).
     */
    static void encodeText(CharBuffer in, ByteBuffer out, Encoding encoding) throws StoreException
    {
        if (encoding == Encoding.UTF8_STRING)
        {
            final CharsetEncoder encoder = getInstance().utf8Encoder;

            encoder.reset().encode(in, out, true);
            encoder.flush(out);
        }
        else
        {
            throw new StoreException("illegal combination of encoding " + encoding.name() + " and value of type CharBuffer");
        }
    }

    /**
     * Encode a field of int[] type.
     *
     * @param in       The int array containing the integers to encode.
     * @param out      Buffer to write the compressed data to.
     * @param encoding The encoding to use (must a compatible with class of input).
     */
    static void encodeIntegers(int[] in, ByteBuffer out, Encoding encoding) throws StoreException
    {
        final int[] outBuffer = new int[in.length + in.length / 2];
        final IntWrapper outPos = new IntWrapper();
        final Encoder encoder = getInstance();
        final IntBuffer outIntBuffer = out.asIntBuffer();
        switch (encoding)
        {
            case INT32_VAR_BYTE_FAST_PFOR:
            {
                // TODO Optimize this by making compression directly support ByteBuffer/IntBuffer as input/output or use array()
                encoder.INT32_VAR_BYTE_FAST_PFOR.headlessCompress(in, new IntWrapper(0), in.length, outBuffer, outPos);
                break;
            }

            case INT32_DELTA_VAR_BYTE_FAST_PFOR:
            {
                // TODO Optimize this by making compression directly support ByteBuffer/IntBuffer as input/output or use array()
                encoder.INT32_DELTA_VAR_BYTE_FAST_PFOR.headlessCompress(in, new IntWrapper(), in.length, outBuffer, outPos);
                break;
            }

            default:
                throw new StoreException("illegal combination of encoding " + encoding.name() + " and value of type int[]");
        }

        final int outPosition = outPos.get();
        outIntBuffer.put(outBuffer, 0, outPosition);
        out.position(out.position() + outPosition * 4);
    }

    private static Encoder getInstance()
    {
        return INSTANCES.get();
    }


    private static final class DeltaSkippableComposition extends SkippableComposition
    {
        /**
         * Compose a scheme from a first one (f1) and a second one (f2). The first
         * one is called first and then the second one tries to compress whatever
         * remains from the first run.
         * <p>
         * By convention, the first scheme should be such that if, during decoding,
         * a 32-bit zero is first encountered, then there is no output.
         *
         * @param f1 first codec
         */
        DeltaSkippableComposition(SkippableIntegerCODEC f1, SkippableIntegerCODEC f2)
        {
            super(f1, f2);
        }


        @Override
        public void headlessCompress(int[] in, IntWrapper inpos, int inlength, int[] out, IntWrapper outpos)
        {
            final int start = inpos.get();

            // encode delta, first value is not touched and used as initial value
            if (inlength > 1)
            {
                Delta.delta(in, start + 1, inlength - 1, in[start]);
            }

            // perform other compression steps
            super.headlessCompress(in, inpos, inlength, out, outpos);
        }


        @Override
        public void headlessUncompress(int[] in, IntWrapper inpos, int inlength, int[] out, IntWrapper outpos, int num)
        {
            // reverse other compression steps
            final int start = outpos.get();
            super.headlessUncompress(in, inpos, inlength, out, outpos, num);

            // reverse delta encoding
            final int end = outpos.get();
            if (num > 1)
            {
                Delta.fastinverseDelta(out, start + 1, end - start - 1, out[start]);
            }
        }
    }


    private static class HeapFastPFOR extends FastPFOR
    {
        /**
         * Create HeapByteBuffer (direct buffers are only really useful in combination with native libs or native IO).
         */
        @Override
        protected ByteBuffer makeBuffer(int sizeInBytes)
        {
            return ByteBuffer.allocate(sizeInBytes).order(ByteOrder.LITTLE_ENDIAN);
        }
    }


    private static class HeapVariableByte extends VariableByte
    {
        /**
         * Create HeapByteBuffer (direct buffers are only really useful in combination with native libs or native IO).
         */
        @Override
        protected ByteBuffer makeBuffer(int sizeInBytes)
        {
            return ByteBuffer.allocate(sizeInBytes).order(ByteOrder.LITTLE_ENDIAN);
        }
    }
}
