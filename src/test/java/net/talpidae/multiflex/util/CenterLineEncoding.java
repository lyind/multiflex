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
public class CenterLineEncoding
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

        // TODO Inline simplifiedCenter() for performance
        final int[] simplified = Curve.simplifiedCenter(curve);
        final int simplifiedEnd = simplified.length;
        final int curveEnd = curve.length;
        final int[] out = new int[Math.max(4, curveEnd * 2)];

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
                    //out[iOut] = yt - curve[i + k];
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

                final int signIntersect = encoded[i];
                ++i;
                final int signIntersectRange = signIntersect >>> 1;
                final int iSignIntersect;
                int sign;
                if (signIntersectRange != 0 && signIntersectRange <= count + 1)
                {
                    // sign change within range, invert for initial values
                    iSignIntersect = i + signIntersectRange;
                    sign = (signIntersect & 1) != 0 ? 1 : -1;
                }
                else
                {
                    // no sign change, first values sign == last values sign
                    iSignIntersect = i + count;
                    sign = (signIntersect & 1) != 0 ? -1 : 1;
                }

                // literal of count length
                final int x1 = x0 + count;
                if (x1 >= curve.length)
                {
                    throw new EncodingException("literal x1 " + x1 + " is larger or equal to curve.length " + curve.length + " at index " + (i - 1));
                }

                for (; x0 < x1; ++x0, ++i)
                {
                    if (i == iSignIntersect)
                    {
                        // switch sign
                        sign *= -1;
                    }

                    curve[x0] = encoded[i] * sign;
                }

                y0 = curve[x1 - 1];
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

                // sign intersect
                final int signIntersect = encoded[i];
                ++i;
                final int signIntersectRange = signIntersect >>> 1;
                final int iSignIntersect;
                int sign;
                final int lastValueSign;
                if (signIntersectRange != 0 && signIntersectRange < count + 1)
                {
                    // sign change within range, invert for initial values
                    iSignIntersect = signIntersectRange;
                    sign = lastValueSign = (signIntersect & 1) != 0 ? 1 : -1;
                }
                else
                {
                    // no sign change, first values sign == last values sign
                    iSignIntersect = count; // include y value
                    lastValueSign = (signIntersect & 1) != 0 ? -1 : 1;
                    sign = lastValueSign * -1;
                }

                // target y
                final int y1 = encoded[i] * (lastValueSign * -1);
                //final int y1 = encoded[i];
                ++i;

                final long y0Fp = fromInt(y0);
                final long y1Fp = fromInt(y1);
                for (int k = 0; k < count; ++k, ++i)
                {
                    if (i == iSignIntersect)
                    {
                        // switch sign
                        sign *= -1;
                    }

                    // lerp8 x0,y0 -> x1,y1
                    final short fraction8bitX0toX1 = (short) ((k << 8) / count);
                    final int yt = toInt(lerp8(y0Fp, y1Fp, fraction8bitX0toX1));

                    // store delta to lerp'd line
                    //curve[x0 + k] = yt - encoded[i];
                    curve[x0 + k] = yt - (encoded[i] * sign);
                }

                // this point is the next lines starting point
                y0 = y1;
                x0 = x1;
            }
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
