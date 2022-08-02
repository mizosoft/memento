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

package com.github.mizosoft.memento;

import com.github.mizosoft.memento.internal.store.DiskStore;
import com.github.mizosoft.memento.internal.store.MemoryStore;
import java.nio.file.Path;
import java.util.concurrent.Executors;

public class Memento {
  private Memento() {}

  public static Store onDisk(Path directory, long maxSize) {
    return DiskStore.newBuilder()
        .directory(directory)
        .executor(
            Executors.newCachedThreadPool(
                r -> {
                  var t = new Thread(r);
                  t.setDaemon(true);
                  return t;
                }))
        .maxSize(maxSize)
        .appVersion(1)
        .build();
  }

  public static Store onMemory(long maxSize) {
    return new MemoryStore(maxSize);
  }
}
