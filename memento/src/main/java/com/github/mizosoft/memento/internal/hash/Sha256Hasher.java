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

package com.github.mizosoft.memento.internal.hash;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class Sha256Hasher implements Hasher {
  private static final MessageDigest SHA_256_DIGEST_TEMPLATE = lookupSha256Digest();
  private static final boolean CLONE_SUPPORTED;

  static {
    boolean cloneSupported;
    try {
      SHA_256_DIGEST_TEMPLATE.clone();
      cloneSupported = true;
    } catch (CloneNotSupportedException ignored) {
      cloneSupported = false;
    }

    CLONE_SUPPORTED = cloneSupported;
  }

  public Sha256Hasher() {}

  @Override
  public byte[] hash(ByteBuffer key) {
    var digest = newDigest();
    digest.update(key);
    return digest.digest();
  }

  private static MessageDigest newDigest() {
    if (CLONE_SUPPORTED) {
      try {
        var digest = (MessageDigest) SHA_256_DIGEST_TEMPLATE.clone();
        digest.reset();
        return digest;
      } catch (CloneNotSupportedException ignored) {
        // Fallback to look-up
      }
    }
    return lookupSha256Digest();
  }

  private static MessageDigest lookupSha256Digest() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new UnsupportedOperationException("SHA-256 not available!", e);
    }
  }
}
