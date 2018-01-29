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

import static net.talpidae.multiflex.store.util.Q32dot8.fromInt;
import static net.talpidae.multiflex.store.util.Q32dot8.toInt;


public class Curve
{
    public static final int[] EMPTY_INT_ARRAY = new int[0];


    public static int[] firstDerivation(int[] curve)
    {
        final int n = curve.length;

        if (n < 2)
        {
            return EMPTY_INT_ARRAY;
        }

        // pick first value
        return firstDerivation(curve[0], curve, 1, n);
    }


    public static int[] firstDerivation(int curveInitial, int[] curve, int offset, int end)
    {
        final int n = end - offset;

        // n < 0 is an error and will cause an exception later on
        if (n == 0)
        {
            return EMPTY_INT_ARRAY;
        }

        final int[] derivation = new int[n];
        int vPrevious = curveInitial;
        for (int i = offset, j = 0; i < end; ++i, ++j)
        {
            final int v = curve[i];
            derivation[j] = v - vPrevious;
            vPrevious = v;
        }

        return derivation;
    }


    public static int[] reverseFirstDerivation(int curveInitial, int[] firstDerivation, int offset, int end)
    {
        final int n = end - offset;
        final int[] curve = new int[n + 1];

        int vPrevious = curve[0] = curveInitial;
        for (int i = offset, j = 1; i < end; ++i, ++j)
        {
            final int v = vPrevious + firstDerivation[i];
            curve[j] = v;
            vPrevious = v;
        }

        return curve;
    }


    public static int[] secondDerivation(int[] curve)
    {
        final int n = curve.length;

        if (n < 3)
        {
            return EMPTY_INT_ARRAY;
        }

        // pre-calculate first delta
        final int v0 = curve[0];
        final int vInitial = curve[1];
        final int d1Initial = vInitial - v0;

        return secondDerivation(vInitial, d1Initial, curve, 2, n);
    }


    public static int[] secondDerivation(int curveInitial, int firstDerivationInitial, int[] curve, int offset, int end)
    {
        final int n = end - offset;

        if (n == 0)
        {
            return EMPTY_INT_ARRAY;
        }

        final int[] derivation = new int[n];
        int vPrevious = curveInitial;
        int d1Previous = firstDerivationInitial;
        for (int i = offset, j = 0; i < end; ++i, ++j)
        {
            final int v = curve[i];
            final int d1 = v - vPrevious;
            vPrevious = v;

            derivation[j] = d1 - d1Previous;
            d1Previous = d1;
        }

        return derivation;
    }


    public static int[] reverseSecondDerivation(int curveInitial, int firstDerivationInitial, int[] secondDerivation, int offset, int end)
    {
        final int n = end - offset;
        final int[] curve = new int[n + 2];

        int vPrevious = curve[0] = curveInitial;
        int d1Previous = firstDerivationInitial;
        curve[1] = vPrevious = vPrevious + d1Previous;
        for (int i = offset, j = 2; i < end; ++i, ++j)
        {
            final int d1 = d1Previous + secondDerivation[i];
            d1Previous = d1;

            final int v = vPrevious + d1;
            curve[j] = vPrevious = v;
        }

        return curve;
    }


    public static int[] upperEnvelope(int[] curve)
    {
        final int n = curve.length;
        if (n < 3)
        {
            return EMPTY_INT_ARRAY;
        }

        final int[] envelopeXY = new int[n * 2];
        int vPrevious = curve[1];
        int d1Previous = curve[1] - curve[0];
        int j = 0;
        for (int i = 2; i < n; ++i)
        {
            final int v = curve[i];
            final int d1 = v - vPrevious;
            if (d1 <= 0 && (d1Previous > 0 || (d1Previous == 0 && j > 0 && envelopeXY[j - 1] == v)))
            {
                // found peak
                envelopeXY[j] = i - 1;
                envelopeXY[j + 1] = curve[i - 1];
                j += 2;
            }

            vPrevious = v;
            d1Previous = d1;
        }

        return Arrays.copyOfRange(envelopeXY, 0, j);
    }


    public static int[] lowerEnvelope(int[] curve)
    {
        final int n = curve.length;
        if (n < 3)
        {
            return EMPTY_INT_ARRAY;
        }

        final int[] envelopeXY = new int[n * 2];
        int vPrevious = curve[1];
        int d1Previous = curve[1] - curve[0];
        int j = 0;
        for (int i = 2; i < n; ++i)
        {
            final int v = curve[i];
            final int d1 = v - vPrevious;
            if (d1 >= 0 && (d1Previous < 0 || (d1Previous == 0 && j > 0 && envelopeXY[j - 1] == v)))
            {
                // found peak
                envelopeXY[j] = i - 1;
                envelopeXY[j + 1] = curve[i - 1];
                j += 2;
            }

            vPrevious = v;
            d1Previous = d1;
        }

        return Arrays.copyOfRange(envelopeXY, 0, j);
    }


    public static int[] center(int[] curve)
    {
        final int n = curve.length;
        if (n < 3)
        {
            return EMPTY_INT_ARRAY;
        }

        final int[] centerXY = new int[n * 2];
        int vPrevious = curve[1];
        int d1Previous = curve[1] - curve[0];
        int xPeakPrevious = 0;
        int yPeakPrevious = 0;
        boolean havePeak = false;
        int j = 0;
        for (int i = 2; i < n; ++i)
        {
            final int v = curve[i];
            final int d1 = v - vPrevious;
            if (d1 <= 0 && (d1Previous > 0 || (d1Previous == 0 && havePeak && yPeakPrevious == v))
                    || d1 >= 0 && (d1Previous < 0 || (d1Previous == 0 && havePeak && yPeakPrevious == v)))
            {
                // found high or low peak
                final int xPeak = i - 1;
                final int yPeak = curve[xPeak];

                if (havePeak)
                {
                    centerXY[j] = xPeak - ((xPeak - xPeakPrevious) / 2); // between this and the previous peak
                    centerXY[j + 1] = (int) (((long) yPeak + (long) yPeakPrevious) / 2); // average
                    j += 2;
                }
                else
                {
                    havePeak = true;
                }

                xPeakPrevious = xPeak;
                yPeakPrevious = yPeak;
            }

            vPrevious = v;
            d1Previous = d1;
        }

        return Arrays.copyOfRange(centerXY, 0, j);
    }


    // find slope where center epsilon (to the interpolated line through from the first through the second center)
    // is lower than the center delta
    public static int[] simplifiedCenter(int[] curve)
    {
        final int n = curve.length;
        if (n < 3)
        {
            return EMPTY_INT_ARRAY;
        }

        final int[] lineXY = new int[n * 2];
        int vPrevious = curve[1];
        int d1Previous = curve[1] - curve[0];
        int xPeakPrevious = 0;
        int yPeakPrevious = vPrevious;
        int xCenter0 = 0;
        int yCenter0 = curve[0];
        int xCenter1 = 0;
        int yCenter1 = 0;
        int yCenterPrevious = 0;
        int peakCount = 1; // fake first peak at x == 0

        // emit first point
        lineXY[0] = xCenter0;
        lineXY[1] = yCenter0;

        int j = 2;
        for (int i = 2; i < n; ++i)
        {
            final int v = curve[i];
            final int d1 = v - vPrevious;
            if (d1 <= 0 && (d1Previous > 0 || (d1Previous == 0 && peakCount > 0 && yPeakPrevious == v))
                    || d1 >= 0 && (d1Previous < 0 || (d1Previous == 0 && peakCount > 0 && yPeakPrevious == v)))
            {
                // found high or low peak
                final int xPeak = i - 1;
                final int yPeak = curve[xPeak];
                final int xCenter = xPeak - ((xPeak - xPeakPrevious) / 2); // between this and the previous peak
                final int yCenter = (int) (((long) yPeak + (long) yPeakPrevious) / 2); // average

                if (peakCount == 1)
                {
                    // register second peak
                    xCenter1 = xCenter;
                    yCenter1 = yCenter;
                }

                final int expectedCenterY = toInt(Q32dot8.lerp8(fromInt(yCenter0), fromInt(yCenter1), (((xCenter - xCenter0) << 8) / (xCenter1 - xCenter0)) - 1));
                final int centerDevitation = yCenter - expectedCenterY;

                // decide if this center should be merged or the line emitted
                // is the difference to the expectedCenterY higher than the delta from the last center?
                final int centerDelta = yCenter - yCenterPrevious;
                if (Math.abs(centerDevitation) > Math.abs(centerDelta))
                {
                    // emit line
                    lineXY[j] = xCenter;
                    lineXY[j + 1] = yCenter;
                    //lineY[j] = expectedCenterY;
                    j += 2;

                    // got new first peak
                    peakCount = 0;
                    xCenter0 = xCenter;
                    yCenter0 = yCenter;
                    //yCenter0 = expectedCenterY;
                }
                else
                {
                    // update second peak (extend line)
                    xCenter1 = xCenter;
                    yCenter1 = yCenter;
                }

                ++peakCount;

                xPeakPrevious = xPeak;
                yPeakPrevious = yPeak;
                yCenterPrevious = yCenter;
            }

            vPrevious = v;
            d1Previous = d1;
        }

        // consider emitting final line
        lineXY[j] = n - 1;
        lineXY[j + 1] = vPrevious;
        j += 2;

        return Arrays.copyOfRange(lineXY, 0, j);
    }


    public static int[] addSaturated(int[] curveA, int[] curveB)
    {
        final int n = Math.min(curveA.length, curveB.length);
        final int[] curve = new int[n];

        for (int i = 0; i < n; ++i)
        {
            curve[i] = (int) Math.min(Integer.MAX_VALUE, Math.max(Integer.MIN_VALUE, (long) curveA[i] + curveB[i]));
        }

        return curve;
    }
}
