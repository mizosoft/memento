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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;

// TODO remove this when the tests are integrated from Methanol
public class CheapTest {
  public static void main(String[] args) throws Exception {
    try (var store = Memento.onDisk(Path.of("test"), 1000)) {
      test(store);
    }
    try (var store = Memento.onMemory(1000)) {
      test(store);
    }

    System.out.println("Passed!");
  }

  private static void test(Store store) throws IOException {
    try (var editor = store.tryEdit("k").orElseThrow();
        var writer = editor.writer()) {
      writer.metadata(UTF_8.encode("Optimus Prime"));
      writer.write(UTF_8.encode("Autobots, Assemble!"));
      editor.commitOnClose();
    }

    try (var viewer = store.viewIfPresent("k").orElseThrow();
        var reader = viewer.reader()) {
      var metadata = UTF_8.decode(reader.metadata()).toString();
      var data = new BufferedReader(viewer.reader().toCharReader(UTF_8)).readLine();
      if (!metadata.equals("Optimus Prime") || !data.equals("Autobots, Assemble!")) {
        throw new AssertionError("ops: " + metadata + " - " + data);
      }
    }
  }
}
