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

/**
 * Implement some simple fixed-point ops for Q32.8 format.
 * <p>
 * The advantage versus floating point is reproducible results on all supported platforms.
 */
public class Q32dot8
{
    public static final long MAX_VALUE = ((long) Integer.MAX_VALUE << 8) | 0xFFL;

    public static final long MIN_VALUE = ((long) Integer.MIN_VALUE << 8) | 0xFFL;

    public static final long OVERFLOW = 0x8000000000000000L;

    private static final long ONE = 0x100;

    private Q32dot8()
    {

    }


    public static long fromInt(int i)
    {
        return i * ONE;
    }


    public static long fromDouble(double d)
    {
        double dTemp = d * ONE;

        return Math.min(MAX_VALUE, Math.max(MIN_VALUE, (long) dTemp));
    }


    public static int toInt(long q32dot8)
    {
        return (int) q32dot8 >> 8;
    }


    /**
     * Linear interpolate from q32dot8a to q32dot8b for the specified fraction.
     *
     * Calculate fraction using: (x &lt;&lt; 8) / range
     */
    public static long lerp8(long q32dot8a, long q32dot8b, int fraction8bit)
    {
        long tempOut = q32dot8a * (ONE - (fraction8bit & 0xFFFF));
        tempOut = tempOut + (q32dot8b * (fraction8bit & 0xFFFF));
        tempOut = tempOut >> 8;
        return tempOut & 0xFFFFFFFFFFL;
    }


    public static long mul(long q32dot8a, long q32dot8b)
    {
        long product = (q32dot8a * q32dot8b) >> 8;

        // check for overflow by comparing the 24 upper most bits (sign)
        int upper = (int) (product >> 39);
        if (product < 0)
        {
            if (~upper != 0)
                return OVERFLOW;
        }
        else
        {
            if (upper != 0)
                return OVERFLOW;
        }

        return product;
    }


    /**
     * Saturated multiply.
     */
    public static long smul(long q32dot8a, long q32dot8b)
    {
        final long result = mul(q32dot8a, q32dot8b);
        if (result == OVERFLOW)
        {
            if ((q32dot8a >= 0) == (q32dot8b >= 0))
            {
                return MAX_VALUE;
            }
            else
            {
                return MIN_VALUE;
            }
        }

        return result;
    }
}
