/*
 * Copyright (c) 2022 Moataz Abdelnasser
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.github.mizonas.memento.internal.hash;

import java.nio.ByteBuffer;
import org.checkerframework.checker.nullness.qual.Nullable;

public class Hex {
  private Hex() {}

  public static String toHexString(ByteBuffer buffer) {
    var sb = new StringBuilder();
    while (buffer.hasRemaining()) {
      byte b = buffer.get();
      char upperHex = Character.forDigit((b >> 4) & 0xf, 16);
      char lowerHex = Character.forDigit(b & 0xf, 16);
      sb.append(upperHex).append(lowerHex);
    }
    return sb.toString();
  }

  public static @Nullable ByteBuffer tryParse(CharSequence hex, int byteCount) {
    if (hex.length() < 2 * byteCount) {
      return null;
    }

    var buffer = ByteBuffer.allocate(byteCount);
    for (int i = 0; i < byteCount; i++) {
      int upperNibble = Character.digit(hex.charAt(2 * i), 16);
      int lowerNibble = Character.digit(hex.charAt(2 * i + 1), 16);
      if (upperNibble == -1 || lowerNibble == -1) {
        return null; // Encountered a non-hex character
      }
      buffer.put((byte) ((upperNibble << 4) | lowerNibble));
    }
    return buffer.flip();
  }
}
