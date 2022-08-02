package com.github.mizonas.memento;

import static com.github.mizonas.memento.internal.Validate.TODO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public interface EntryWriter extends ByteWriter {
  /**
   * Sets the metadata block which is to be applied when the editor is closed.
   *
   * @throws IllegalStateException if the edit is committed
   */
  EntryWriter metadata(ByteBuffer metadata);

  long position() throws IOException;

  EntryWriter position(long position) throws IOException;

  @Override
  int write(ByteBuffer src) throws IOException;

  int write(long position, ByteBuffer src) throws IOException;

  EntryWriter truncate(long size) throws IOException;
}
