package perf;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import org.apache.lucene.document.Document;

public interface DocsProvider extends Closeable {

  IDocState newDocState();

  Document nextDoc(IDocState docState, boolean expected) throws IOException;

  long getBytesIndexed();

  default Document nextDoc(IDocState docState) throws IOException {
    return nextDoc(docState, false);
  }

  default void recycle() {
    // do nothing
  }

  default boolean reserve() {
    // do nothing
    return true;
  }

  class ReaderThread<T> extends Thread {

    private final BlockingQueue<T> outputQueue;
    private final T endMarker;
    private final RunnableWithException readDocs;

    public ReaderThread(BlockingQueue<T> outputQueue, T endMarker, RunnableWithException readDocs) {
      this.outputQueue = outputQueue;
      this.endMarker = endMarker;
      this.readDocs = readDocs;
    }

    @Override
    public void run() {
      try {
        this.readDocs.run();
        for (int i = 0; i < 128; i++) {
          this.outputQueue.put(endMarker);
        }
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }
  }

  @FunctionalInterface
  interface RunnableWithException {
    void run() throws Exception;
  }
}
