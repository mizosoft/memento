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

import com.github.mizosoft.memento.function.ThrowingBiConsumer;
import com.github.mizosoft.memento.function.ThrowingConsumer;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A bounded repository of binary data entries identified by string keys. Each entry has both a
 * metadata block and a data stream. An entry's metadata block and data stream can be accessed by a
 * {@link Viewer} and modified by an {@link Editor}. An entry can have at most one active {@code
 * Editor} but can have multiple concurrent {@code Viewers} after it has been first successfully
 * edited.
 *
 * <p>{@code Store} bounds the size of data it contains to its {@link #maxSize()} by automatic
 * eviction of entries, possibly in background. A store's {@link #size()} might temporarily exceed
 * its bound in case entries are being actively expanded while eviction is in progress. A store's
 * size doesn't include the overhead of the underlying filesystem or any metadata the store itself
 * uses for indexing purposes. Thus, a store's {@code maxSize()} is not exact and might be slightly
 * exceeded as necessary.
 *
 * <p>{@code Store} is thread-safe and is suitable for concurrent usage.
 */
public interface Store extends AutoCloseable, Flushable {

  /** Returns this store's max size in bytes. */
  long maxSize();

  /** Returns the optional executor used for background operations. */
  Optional<Executor> executor();

  Optional<Path> directory();

  /** Initializes this store. */
  void initialize() throws IOException;

  /** Asynchronously initializes this store. */
  CompletableFuture<Void> initializeAsync();

  /**
   * Returns a {@code Viewer} for the entry associated with the given key, or an empty {@code
   * Optional} if there's no such entry.
   *
   * @throws IllegalStateException if the store is closed
   */
  Optional<Viewer> viewIfPresent(String key) throws IOException;

  Viewer view(String key) throws IOException, InterruptedException;

  /**
   * Returns an {@code Editor} for the entry associated with the given key (atomically creating a
   * new one if necessary), or an empty {@code Optional} if such entry is currently being edited.
   *
   * @throws IllegalStateException if the store is closed
   */
  Optional<Editor> tryEdit(String key) throws IOException;

  Editor edit(String key) throws IOException, InterruptedException;

  Editor edit(String key, Duration timeout)
      throws IOException, InterruptedException, TimeoutException;

//  Viewer generate(
//      String key, ThrowingBiConsumer<? super @Nullable EntryReader, ? super EntryWriter> generator)
//      throws IOException, InterruptedException;

  Viewer generateIfAbsent(
      String key, ThrowingConsumer<? super @Nullable EntryWriter> generator);

  Optional<Viewer> generateIfPresent(
      String key, ThrowingBiConsumer<? super EntryReader, ? super EntryWriter> generator);

  /**
   * Returns an iterator of {@code Viewers} over the entries in this store. The iterator doesn't
   * throw {@code ConcurrentModificationException} when the store is changed but might or might not
   * reflect these changes.
   */
  Iterator<Viewer> iterator() throws IOException;

  /**
   * Removes the entry associated with the given key.
   *
   * @throws IllegalStateException if the store is closed
   */
  boolean remove(String key) throws IOException;

  /**
   * Removes all entries from this store.
   *
   * @throws IllegalStateException if the store is closed
   */
  void clear() throws IOException;

  /** Returns the size in bytes of all entries in this store. */
  long size() throws IOException;

  /** Atomically clears and closes this store. */
  void dispose() throws IOException;

  /**
   * Closes this store. Once the store is closed, all ongoing edits will silently fail to write or
   * commit anything.
   */
  @Override
  void close() throws IOException;

  /**
   * Flushes any data buffered by this store.
   *
   * @throws IllegalStateException if the store is closed
   */
  @Override
  void flush() throws IOException;

  /** Reads an entry's metadata block and data stream. */
  interface Viewer extends Closeable {

    /** Returns the entry's key. */
    String key();

    EntryReader reader();

    /** Returns the size in bytes of the data stream. */
    long dataSize();

    /** Returns the size in bytes of the metadata block and data stream. */
    long entrySize();

    /**
     * Returns an editor for this viewer's entry, or {@code null} if another edit is in progress or
     * if the entry has been modified since this viewer was created. Changes made by the returned
     * editor are not reflected by this viewer.
     *
     * @throws IllegalStateException if the store is closed
     */
    Optional<Editor> tryEdit() throws IOException;

    Editor edit() throws IOException, InterruptedException;

    /**
     * Removes the entry associated with this viewer only if it hasn't changed since this viewer was
     * opened.
     *
     * @throws IllegalStateException if the store is closed
     */
    boolean removeEntry() throws IOException;

    /** Closes this viewer. */
    @Override
    void close();
  }

  /** Writes an entry's metadata block and data stream. */
  interface Editor extends Closeable {

    /** Returns the entry's key. */
    String key();

    EntryWriter writer();

    //    Viewer view();

    /**
     * Marks the edit as committed so that modifications made so far are applied when the editor is
     * closed.
     *
     * @throws IllegalStateException if the editor is closed
     */
    void commitOnClose();

    /**
     * Closes this editor. If {@link #commitOnClose()} is called, changes made by this editor are
     * applied; otherwise, they're discarded.
     */
    @Override
    void close() throws IOException;
  }
}
