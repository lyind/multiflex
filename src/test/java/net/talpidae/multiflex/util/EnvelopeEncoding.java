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

package net.talpidae.multiflex.util;


import net.talpidae.multiflex.store.util.Curve;

import java.util.Arrays;

import static net.talpidae.multiflex.store.util.Curve.EMPTY_INT_ARRAY;
import static net.talpidae.multiflex.util.Q32dot8.*;


/**
 * Arithmetic filter for making signals more compressible using integer patching techniques.
 */
public class EnvelopeEncoding
{
    // minimal number of samples per block (otherwise overhead is too high)
    private static final int MINIMAL_BLOCK_SIZE = 1;

    private static final int MAX_INPUT_LENGTH = Integer.MAX_VALUE >> 2;

    public static int[] encode(int[] curve)
    {
        if (curve.length > MAX_INPUT_LENGTH)
        {
            throw new IllegalArgumentException("input curve longer than " + MAX_INPUT_LENGTH);
        }

        // TODO Inline this for performance
        final int[] simplified = calculateTraceCurveSimplified(curve);

        final int simplifiedEnd = simplified.length;
        final int curveEnd = curve.length;
        final int[] out = new int[Math.max(6, curveEnd  * 2)];

        // consume first point
        int x0 = 0;
        int x1 = simplified[0];
        int j = 2;
        int y0 = simplified[1];

        // emit initial literal of size 1
        out[0] = 0x1;
        out[1] = y0 >>> 31;
        out[2] = (y0 >>> 31) != 0 ? y0 * -1 : y0;
        int iOut = 3;
        for (int i = 1; i < curveEnd; )
        {
            // consume next point
            x0 = x1 + 1;
            //x1 = simplified[Math.min(simplifiedEnd - 2, j)];
            x1 = simplified[j];
            j += 2;

            // we need at least a range of ~8 compressed samples for encoding to make sense
            int count = x1 - x0 + 1;
            if (count < MINIMAL_BLOCK_SIZE)
            {
                // merge literal samples
                for (; j < simplifiedEnd; j += 2)
                {
                    final int candidateX0 = x1;
                    final int candidateX1 = simplified[j];
                    final int blockCount = candidateX1 - candidateX0;
                    if (blockCount < MINIMAL_BLOCK_SIZE)
                    {
                        j += 2;
                        x1 = candidateX1;
                        count += blockCount;
                    }
                    else
                    {
                        break;
                    }
                }

                y0 = curve[j - 1];

                // store odd range == literal length shifted by 1
                out[iOut] = 0x1 | ((count - 1) << 1);
                ++iOut;

                // first negative indicator (range shifted by 1)
                // if the range is 0, it must be set to the total range as there is no sign change at all
                // if bit 0 is set, all numbers until this index are multiplied by -1
                final int iOutSignIntersect = iOut;
                ++iOut;

                int iSignIntersect = 0;
                int sign = 0;
                final int begin = i;
                final int literalEnd = i + count;
                for (; i < literalEnd; ++i)
                {
                    final int literal = curve[i];
                    if (iSignIntersect == 0)
                    {
                        if (literal > 0)
                        {
                            if (sign < 0)
                            {
                                iSignIntersect = i - begin;
                            }
                            sign = 1;
                        }
                        else if (literal < 0)
                        {
                            if (sign > 0)
                            {
                                iSignIntersect = i - begin;
                            }
                            sign = -1;
                        }
                        else
                        {
                            sign = 1;
                        }
                    }

                    out[iOut] = literal * sign;
                    ++iOut;
                }

                // store sign intersect
                out[iOutSignIntersect] = (iSignIntersect << 1) | (sign >>> 31);
            }
            else
            {
                // store even range == (compressed length - MINIMAL_BLOCK_SIZE) shifted by 1
                out[iOut] = (count - MINIMAL_BLOCK_SIZE) << 1;
                ++iOut;

                // first negative indicator (range shifted by 1)
                // if the range is 0, it must be set to the total range as there is no sign change at all
                // if bit 0 is set, all numbers until this index are multiplied by -1
                final int iOutSignIntersect = iOut;
                ++iOut;

                // reserve space to store target y
                final int y1 = simplified[j - 1];
                ++iOut;

                int iSignIntersect = 0;
                int sign = 0;
                final long y0Fp = fromInt(y0);
                final long y1Fp = fromInt(y1);
                for (int k = 0; k < count; ++k)
                {
                    // lerp8 x0,y0 -> x1,y1
                    final short fraction8bitX0toX1 = (short) ((k << 8) / count);
                    final int yt = toInt(lerp8(y0Fp, y1Fp, fraction8bitX0toX1));

                    // store delta to lerp'd line
                    final int deltaYt = yt - curve[i + k];
                    if (iSignIntersect == 0)
                    {
                        if (deltaYt > 0)
                        {
                            if (sign < 0)
                            {
                                iSignIntersect = k;
                            }
                            sign = 1;
                        }
                        else if (deltaYt < 0)
                        {
                            if (sign > 0)
                            {
                                iSignIntersect = k;
                            }
                            sign = -1;
                        }
                        else
                        {
                            sign = 1;
                        }
                    }

                    out[iOut] = deltaYt * sign;
                    ++iOut;
                }

                // handle target y as the last value of the line
                if (iSignIntersect == 0)
                {
                    if (y1 > 0)
                    {
                        if (sign < 0)
                        {
                            iSignIntersect = count;
                        }
                        sign = 1;
                    }
                    else if (y1 < 0)
                    {
                        if (sign > 0)
                        {
                            iSignIntersect = count;
                        }
                        sign = -1;
                    }
                    else
                    {
                        sign = 1;
                    }
                }

                // store sign intersect
                out[iOutSignIntersect] = (iSignIntersect << 1) | (sign >>> 31);
                out[iOutSignIntersect + 1] = y1 * (sign * -1);

                i += count; // we also output the next value (the target y)
                y0 = y1;
            }
        }

        return Arrays.copyOf(out, iOut);
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
            final int range = encoded[i];
            ++i;

            if ((range & 0x1) != 0)
            {
                final int count = (range >> 1) + 1;
                if (count <= 0)
                {
                    throw new EncodingException("literal range " + count + " at index " + (i - 1) + " is zero or negative");
                }

                // literal of count length
                final int x1 = x0 + count;
                if (x1 >= curve.length)
                {
                    throw new EncodingException("literal x1 " + x1 + " is larger or equal to curve.length " + curve.length + " at index " + (i - 1));
                }

                for (; x0 < x1; ++x0, ++i)
                {
                    curve[x0] = encoded[i];
                }

                y0 = encoded[i - 1];
            }
            else if (i < encoded.length)
            {
                final int count = (range >> 1) + MINIMAL_BLOCK_SIZE;
                if (count < MINIMAL_BLOCK_SIZE)
                {
                    throw new EncodingException("line range " + count + " at index " + (i - 1) + " is smaller than " + MINIMAL_BLOCK_SIZE);
                }

                // line length: count + MINIMAL_BLOCK_SIZE
                final int x1 = x0 + count;
                if (x1 >= curve.length)
                {
                    throw new EncodingException("line x1 " + x1 + " is larger or equal to curve.length " + curve.length + " at index " + (i - 1));
                }

                final long y0Fp = fromInt(y0);

                // target y
                final int y1 = encoded[i];
                ++i;

                final long y1Fp = fromInt(y1);
                for (int k = 0; k < count; ++k, ++i)
                {
                    // lerp8 x0,y0 -> x1,y1
                    final short fraction8bitX0toX1 = (short) ((k << 8) / count);
                    final int yt = toInt(lerp8(y0Fp, y1Fp, fraction8bitX0toX1));

                    // store delta to lerp'd line
                    curve[x0 + k] = yt - encoded[i];
                }

                // this point is the next lines starting point
                y0 = y1;
                x0 = x1;
            }
        }

        return Arrays.copyOf(curve, x0);
    }


    public static int[] calculateTraceCurveSimplified(int[] curve)
    {
        final int[] upperEnvelope = Curve.upperEnvelope(curve);
        final int[] lowerEnvelope = Curve.lowerEnvelope(curve);

        int iHigh = 0;
        int iLow = 0;
        final int[] trace = new int[upperEnvelope.length + lowerEnvelope.length + 4];

        // insert start and end point
        trace[0] = 0;
        trace[1] = curve[0];
        trace[trace.length - 2] = curve.length - 1;
        trace[trace.length - 1] = curve[curve.length - 1];

        // join high and low peaks
        for (int i = 2; i < trace.length - 2; i += 2)
        {
            // find next x
            final int xHigh = iHigh < upperEnvelope.length ? upperEnvelope[iHigh] : Integer.MAX_VALUE;
            final int xLow = iLow < lowerEnvelope.length ? lowerEnvelope[iLow] : Integer.MAX_VALUE;

            if (xHigh < xLow)
            {
                trace[i] = xHigh;
                trace[i + 1] = upperEnvelope[iHigh + 1];
                iHigh += 2;
            }
            else
            {
                trace[i] = xLow;
                trace[i + 1] = lowerEnvelope[iLow + 1];
                iLow += 2;
            }
        }

        return trace;
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
