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

package net.talpidae.multiflex.store.util;


import java.util.Arrays;

import static net.talpidae.multiflex.store.util.Curve.EMPTY_INT_ARRAY;
import static net.talpidae.multiflex.store.util.Q32dot8.*;


/**
 * Arithmetic filter that makes signals more compressible (for use with integer patching techniques).
 * <p>
 * IMPORTANT: This filter does not support the full integer range,
 * ie. the number -2147483648 (Integer.MIN_VALUE) can not be stored.
 */
public class Center31BitEncoding
{
    private static final int MAX_INPUT_LENGTH = Integer.MAX_VALUE >> 2;

    public static int[] encode(int[] curve)
    {
        if (curve.length > MAX_INPUT_LENGTH)
        {
            throw new IllegalArgumentException("input curve longer than " + MAX_INPUT_LENGTH);
        }

        // TODO Inline simplifiedCenter() for performance
        final int[] simplified = Curve.simplifiedCenter(curve);
        final int curveEnd = curve.length;
        final int[] out = new int[Math.max(4, curveEnd * 2)];

        // consume first point
        int x0 = 0;
        int x1 = simplified[0];
        int j = 2;
        int y0 = simplified[1];

        // emit initial literal of size 1
        out[0] = 0;
        out[1] = toShifted(y0);
        int iOut = 2;
        for (int i = 1; i < curveEnd; )
        {
            // consume next point
            x0 = x1 + 1;
            x1 = simplified[j];
            j += 2;

            final int count = x1 - x0 + 1;

            // store range
            out[iOut] = count - 1;
            ++iOut;

            // reserve space to store target y
            final int y1 = simplified[j - 1];
            out[iOut] = toShifted(y1);
            ++iOut;

            final long y0Fp = fromInt(y0);
            final long y1Fp = fromInt(y1);
            for (int k = 0; k < count - 1; ++k)
            {
                // lerp8 x0,y0 -> x1,y1
                final short fraction8bitX0toX1 = (short) ((k << 8) / count);
                final int yt = toInt(lerp8(y0Fp, y1Fp, fraction8bitX0toX1));

                // store delta to lerp'd line
                out[iOut] = toShifted(yt - curve[i + k]);
                ++iOut;
            }

            i += count; // we also output the next value (the target y)
            y0 = y1;
        }

        return Arrays.copyOf(out, iOut);
    }


    // make positive and shift left by one, indicating negative sign with a 1 at bit0
    private static int toShifted(int i)
    {
        if (i < 0)
        {
            i *= -1;
            if ((i & 0x40000000) != 0)
            {
                throw new EncodingException("number -2147483648 found in input, processing not supported");
            }

            return ((i << 1) & 0x7FFFFFFF) | 0x1;
        }
        else
        {
            return ((i << 1) & 0x7FFFFFFF);
        }
    }

    // reverse toShifted()
    private static int fromShifted(int i)
    {
        if ((i & 1) != 0)
        {
            // negative
            return (i >>> 1) * -1;
        }
        else
        {
            // positive
            return i >>> 1;
        }
    }


    public static int[] decode(int[] encoded) throws EncodingException
    {
        if (encoded.length == 0)
        {
            return EMPTY_INT_ARRAY;
        }

        final int[] curve = new int[encoded.length - 1];
        int x0 = 0;
        int y0 = 0;
        for (int i = 0; i < encoded.length; )
        {
            final int count = encoded[i] + 1;
            final int x1 = x0 + count;
            if (count < 1)
            {
                throw new EncodingException("line range " + count + " at index " + i + " is smaller than 1");
            }
            else if (x1 >= curve.length)
            {
                throw new EncodingException("line x1 " + x1 + " is larger or equal to curve.length " + curve.length + " at index " + i);
            }
            ++i;

            // target y
            final int y1 = fromShifted(encoded[i]);
            ++i;

            final long y0Fp = fromInt(y0);
            final long y1Fp = fromInt(y1);
            for (int k = 0; k < count - 1; ++k, ++i)
            {
                // lerp8 x0,y0 -> x1,y1
                final short fraction8bitX0toX1 = (short) ((k << 8) / count);
                final int yt = toInt(lerp8(y0Fp, y1Fp, fraction8bitX0toX1));

                // store delta to lerp'd line
                curve[x0 + k] = yt - fromShifted(encoded[i]);
            }

            curve[x0 + count - 1] = y1;

            // this point is the next lines starting point
            y0 = y1;
            x0 = x1;
        }

        return Arrays.copyOf(curve, x0);
    }


    public static class EncodingException extends RuntimeException
    {
        private EncodingException(String message)
        {
            super(message);
        }

        private EncodingException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }
}
