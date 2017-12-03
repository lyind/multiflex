package net.talpidae.multiflex.store.sqlite;

import me.lemire.integercompression.*;
import me.lemire.integercompression.differential.Delta;
import net.talpidae.multiflex.format.Encoding;
import net.talpidae.multiflex.store.StoreException;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;


class Encoder
{
    private static ThreadLocal<Encoder> INSTANCES = ThreadLocal.withInitial(Encoder::new);

    private final SkippableComposition INT32_VAR_BYTE_FAST_PFOR;

    private final SkippableComposition INT32_DELTA_VAR_BYTE_FAST_PFOR;

    private final CharsetDecoder utf8Decoder;

    private final CharsetEncoder utf8Encoder;


    private Encoder()
    {
        INT32_VAR_BYTE_FAST_PFOR = new SkippableComposition(new HeapFastPFOR(), new HeapVariableByte());
        INT32_DELTA_VAR_BYTE_FAST_PFOR = new DeltaSkippableComposition(new HeapFastPFOR(), new HeapVariableByte());

        utf8Encoder = StandardCharsets.UTF_8.newEncoder();
        utf8Decoder = StandardCharsets.UTF_8.newDecoder();
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
        final int[] in = new int[data.remaining()];
        data.asIntBuffer().get(in);

        final int[] out = new int[outLength];
        final Encoder encoder = getInstance();

        switch (encoding)
        {
            case INT32_VAR_BYTE_FAST_PFOR:
            {
                // TODO Optimize this by making compression directly support ByteBuffer/IntBuffer as input/output or use array()
                encoder.INT32_VAR_BYTE_FAST_PFOR.headlessUncompress(in, new IntWrapper(0), in.length, out, new IntWrapper(), outLength);
                break;
            }

            case INT32_DELTA_VAR_BYTE_FAST_PFOR:
            {
                // TODO Optimize this by making compression directly support ByteBuffer/IntBuffer as input/output or use array()
                encoder.INT32_DELTA_VAR_BYTE_FAST_PFOR.headlessUncompress(in, new IntWrapper(0), in.length, out, new IntWrapper(), outLength);
                break;
            }

            default:
                throw new StoreException("illegal combination of encoding " + encoding.name() + " and value of type int[]");
        }

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
            inBuffer.put(in);

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

            encoder.encode(in, out, true);
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
        final int[] outBuffer = new int[in.length];
        final IntWrapper outPos = new IntWrapper();
        final Encoder encoder = getInstance();
        switch (encoding)
        {
            case INT32_VAR_BYTE_FAST_PFOR:
            {
                // TODO Optimize this by making compression directly support ByteBuffer/IntBuffer as input/output or use array()
                encoder.INT32_VAR_BYTE_FAST_PFOR.headlessCompress(in, new IntWrapper(0), in.length, outBuffer, outPos);

                out.asIntBuffer().put(outBuffer, 0, outPos.get());
                break;
            }

            case INT32_DELTA_VAR_BYTE_FAST_PFOR:
            {
                // TODO Optimize this by making compression directly support ByteBuffer/IntBuffer as input/output or use array()
                encoder.INT32_DELTA_VAR_BYTE_FAST_PFOR.headlessCompress(in, new IntWrapper(0), in.length, outBuffer, outPos);

                out.asIntBuffer().put(outBuffer, 0, outPos.get());
                break;
            }

            default:
                throw new StoreException("illegal combination of encoding " + encoding.name() + " and value of type int[]");
        }
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
        public DeltaSkippableComposition(SkippableIntegerCODEC f1, SkippableIntegerCODEC f2)
        {
            super(f1, f2);
        }


        @Override
        public void headlessCompress(int[] in, IntWrapper inpos, int inlength, int[] out, IntWrapper outpos)
        {
            final int start = inpos.get();

            // encode delta, first value is not touched and used as initial value
            Delta.delta(in, start + 1, inlength - 1, in[start]);

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
            Delta.fastinverseDelta(out, start + 1, end - start - 1, out[start]);
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
            return ByteBuffer.allocate(sizeInBytes);
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
            return ByteBuffer.allocate(sizeInBytes);
        }
    }
}