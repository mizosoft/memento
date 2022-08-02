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

package com.github.mizosoft.memento.internal;

import java.io.Closeable;
import java.io.IOException;
import java.lang.System.Logger.Level;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.checkerframework.checker.nullness.qual.Nullable;

public class IOUtils {
  private static final System.Logger logger = System.getLogger(IOUtils.class.getName());

  private IOUtils() {}

  public static ByteBuffer copy(ByteBuffer buffer) {
    return ByteBuffer.allocate(buffer.remaining()).put(buffer).flip();
  }

  public static ByteBuffer copy(ByteBuffer source, ByteBuffer target) {
    return target.remaining() >= source.remaining() ? target.put(source) : copy(source);
  }

  public static ByteBuffer copyIfWritable(ByteBuffer buffer) {
    return buffer.isReadOnly() ? buffer : copy(buffer).asReadOnlyBuffer();
  }

  public static int copyRemaining(ByteBuffer src, ByteBuffer dst) {
    int toCopy = Math.min(src.remaining(), dst.remaining());
    int srcLimit = src.limit();
    src.limit(src.position() + toCopy);
    dst.put(src);
    src.limit(srcLimit);
    return toCopy;
  }

  /**
   * Tries to clone & rethrow {@code throwable} to capture the current stack trace, or throws an
   * {@code IOException} with {@code throwable} as its cause if cloning is not possible. Return type
   * is only declared for this method to be conveniently used in a {@code throw} statement.
   */
  private static RuntimeException rethrowAsyncThrowable(
      Throwable throwable, boolean rethrowInterruptedException)
      throws IOException, InterruptedException {
    if (throwable instanceof InterruptedException && !rethrowInterruptedException) {
      throw new IOException(throwable);
    }

    var clonedThrowable = tryCloneThrowable(throwable);
    if (clonedThrowable instanceof RuntimeException) {
      throw (RuntimeException) clonedThrowable;
    } else if (clonedThrowable instanceof Error) {
      throw (Error) clonedThrowable;
    } else if (clonedThrowable instanceof IOException) {
      throw (IOException) clonedThrowable;
    } else if (clonedThrowable instanceof InterruptedException) {
      throw (InterruptedException) clonedThrowable;
    } else {
      throw new IOException(throwable);
    }
  }

  @SuppressWarnings("unchecked")
  private static @Nullable Throwable tryCloneThrowable(Throwable t) {
    // Clone the main cause in a CompletionException|ExecutionException chain
    var throwableToClone = getDeepCompletionCause(t);

    // Don't try cloning if we can't rethrow the cloned exception and we'll end up wrapping it in an
    // IOException anyway.
    if (!(throwableToClone instanceof RuntimeException
        || throwableToClone instanceof Error
        || throwableToClone instanceof IOException
        || throwableToClone instanceof InterruptedException)) {
      return null;
    }

    try {
      for (var constructor :
          (Constructor<? extends Throwable>[]) throwableToClone.getClass().getConstructors()) {
        var paramTypes = constructor.getParameterTypes();
        if (paramTypes.length == 2
            && paramTypes[0] == String.class
            && paramTypes[1] == Throwable.class) {
          return constructor.newInstance(t.getMessage(), t);
        } else if (paramTypes.length == 1 && paramTypes[0] == String.class) {
          return constructor.newInstance(t.getMessage()).initCause(t);
        } else if (paramTypes.length == 1 && paramTypes[0] == Throwable.class) {
          return constructor.newInstance(t);
        } else if (paramTypes.length == 0) {
          return constructor.newInstance().initCause(t);
        }
      }
    } catch (ReflectiveOperationException ignored) {
      // Fallback to null
    }
    return null;
  }

  public static Throwable getDeepCompletionCause(Throwable t) {
    var cause = t;
    while (cause instanceof CompletionException || cause instanceof ExecutionException) {
      var deeperCause = cause.getCause();
      if (deeperCause == null) {
        break;
      }
      cause = deeperCause;
    }
    return cause;
  }

  public static <T> T block(Future<T> future) throws IOException, InterruptedException {
    try {
      return future.get();
    } catch (ExecutionException e) {
      throw rethrowAsyncThrowable(e.getCause(), true);
    }
  }

  /** Same as {@link #block(Future)} but throws an {@code IOException} when interrupted. */
  public static <T> T blockOnIO(Future<T> future) throws IOException {
    try {
      return future.get();
    } catch (ExecutionException e) {
      try {
        throw rethrowAsyncThrowable(e.getCause(), false);
      } catch (InterruptedException t) {
        throw new AssertionError(t);
      }
    } catch (InterruptedException e) {
      throw new IOException("interrupted while waiting for I/O", e);
    }
  }

  public static void closeQuietly(@Nullable Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException e) {
        logger.log(Level.WARNING, "exception while closing: " + closeable, e);
      }
    }
  }
}
