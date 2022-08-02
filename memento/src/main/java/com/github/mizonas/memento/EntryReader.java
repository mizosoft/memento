package com.github.mizonas.memento;

import static com.github.mizonas.memento.internal.Validate.TODO;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;

public interface EntryReader extends ByteReader {
  ByteBuffer metadata();

  long position() throws IOException;

  EntryReader position(long position) throws IOException;

  @Override
  int read(ByteBuffer dst) throws IOException;

  int read(ByteBuffer dst, long position) throws IOException;

  boolean removeEntry() throws IOException;
}
