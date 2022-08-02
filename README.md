# Memento

A high-concurrency key-value store with background LRU eviction based on a size bound.
The store is inspired by [Chromium's Simple Backend](https://www.chromium.org/developers/design-documents/network-stack/disk-cache/very-simple-backend/).

Each entry tracked by the store is mapped by a string key, and comprises a metadata block and a data stream. The entry's metadata block
& data stream can be accessed by a `Viewer` and modified by an `Editor`. An entry can have at most one
active `Editor` but can have multiple concurrent `Viewers` after it has been first successfully edited.

## Download 

Memento is not yet available on maven. You can try out it by cloning this repo, but note that the
API is subject to change.

The library is currently compatible with Java 17+.

## Usage

```java
// Specify a directory and a size bound (in bytes) to create a disk store
try (var store = Memento.onDisk(Path.of("test-dir"), 8 * 1024)) {
  try (var editor = store.tryEdit("key").orElseThrow(); 
       var writer = editor.writer()) {
    writer.metadata(UTF_8.encode("Optimus Prime"));
    writer.write(UTF_8.encode("Autobots, Assemble!"));
    
    // Must invoke commitOnClose so the editor knows what's written 
    // is to be committed when closing.
    editor.commitOnClose();
  }

  try (var viewer = store.viewIfPresent("key").orElseThrow();
       var reader = viewer.reader()) {
    var metadata = UTF_8.decode(reader.metadata()).toString();
    var data = new BufferedReader(viewer.reader().toCharReader(UTF_8)).readLine();
    if (!metadata.equals("Optimus Prime") || !data.equals("Autobots, Assemble!")) {
      throw new AssertionError("ops: " + metadata + " - " + data);
    }
  }
}
```

## Origins

This project originally sprung up from within [Methanol's](https://github.com/mizosoft/methanol) HTTP cache implementation. I originally wanted
to write a good default storage backend for the HTTP cache without relying on third-party dependencies (maintaining Methanol's zero dependency policy, if you want to call it such),
with possibility of expansion to other storage backends (e.g. redis) under the same [API](/memento/src/main/java/com/github/mizosoft/memento/Store.java).

# License

[MIT](https://choosealicense.com/licenses/mit/)
