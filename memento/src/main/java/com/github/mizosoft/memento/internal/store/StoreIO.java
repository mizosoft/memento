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

package com.github.mizosoft.memento.internal.store;

import static java.lang.String.format;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.util.concurrent.CompletableFuture;

/** Read/Write utilities that make sure exactly the requested bytes are read/written. */
public class StoreIO {
  private StoreIO() {}

  static ByteBuffer readNBytes(FileChannel channel, int byteCount) throws IOException {
    return readNBytes(channel, byteCount, -1);
  }

  static ByteBuffer readNBytes(FileChannel channel, int byteCount, long position)
      throws IOException {
    var buffer = ByteBuffer.allocate(byteCount);
    int totalRead = 0;
    for (int read = 0; read >= 0 && buffer.hasRemaining(); ) {
      read = position >= 0 ? channel.read(buffer, position) : channel.read(buffer);
      if (read >= 0) {
        totalRead += read;
        if (position >= 0) {
          position += read;
        }
      }
    }
    if (buffer.hasRemaining()) {
      throw new EOFException(format("expected %d bytes, found %d", byteCount, totalRead));
    }
    return buffer.flip();
  }

  static CompletableFuture<Integer> readBytesAsync(
      AsynchronousFileChannel channel, ByteBuffer dst, long position) {
    var future = new CompletableFuture<Integer>();
    channel.read(dst, position, dst, new ReadCompletionHandler(channel, future, position));
    return future;
  }

  static void writeBytes(FileChannel channel, ByteBuffer src) throws IOException {
    do {
      channel.write(src);
    } while (src.hasRemaining());
  }

  static void writeBytes(FileChannel channel, ByteBuffer src, long position) throws IOException {
    do {
      int written = channel.write(src, position);
      position += written;
    } while (src.hasRemaining());
  }

  static CompletableFuture<Integer> writeBytesAsync(
      AsynchronousFileChannel channel, ByteBuffer src, long position) {
    var future = new CompletableFuture<Integer>();
    channel.write(src, position, src, new WriteCompletionHandler(channel, future, position));
    return future;
  }

  private static final class ReadCompletionHandler
      implements CompletionHandler<Integer, ByteBuffer> {
    private final AsynchronousFileChannel channel;
    private final CompletableFuture<Integer> future;
    private final long position;
    private final int totalRead;

    ReadCompletionHandler(
        AsynchronousFileChannel channel, CompletableFuture<Integer> future, long position) {
      this(channel, future, position, 0);
    }

    private ReadCompletionHandler(
        AsynchronousFileChannel channel,
        CompletableFuture<Integer> future,
        long position,
        int totalRead) {
      this.channel = channel;
      this.future = future;
      this.position = position;
      this.totalRead = totalRead;
    }

    @Override
    public void completed(Integer read, ByteBuffer dst) {
      if (read >= 0 && dst.hasRemaining()) {
        long nextPosition = position + read;
        channel.read(
            dst,
            nextPosition,
            dst,
            new ReadCompletionHandler(channel, future, nextPosition, totalRead + read));
      } else if (totalRead > 0) {
        future.complete(totalRead + Math.max(0, read));
      } else {
        future.complete(read);
      }
    }

    @Override
    public void failed(Throwable exc, ByteBuffer dst) {
      future.completeExceptionally(exc);
    }
  }

  private static final class WriteCompletionHandler
      implements CompletionHandler<Integer, ByteBuffer> {
    private final AsynchronousFileChannel channel;
    private final CompletableFuture<Integer> future;
    private final long position;
    private final int totalWritten;

    WriteCompletionHandler(
        AsynchronousFileChannel channel, CompletableFuture<Integer> future, long position) {
      this(channel, future, position, 0);
    }

    private WriteCompletionHandler(
        AsynchronousFileChannel channel,
        CompletableFuture<Integer> future,
        long position,
        int totalWritten) {
      this.channel = channel;
      this.future = future;
      this.position = position;
      this.totalWritten = totalWritten;
    }

    @Override
    public void completed(Integer written, ByteBuffer src) {
      if (src.hasRemaining()) {
        long nextPosition = position + written;
        channel.write(
            src,
            nextPosition,
            src,
            new WriteCompletionHandler(channel, future, nextPosition, totalWritten + written));
      } else {
        future.complete(totalWritten + written);
      }
    }

    @Override
    public void failed(Throwable exc, ByteBuffer src) {
      future.completeExceptionally(exc);
    }
  }
}
