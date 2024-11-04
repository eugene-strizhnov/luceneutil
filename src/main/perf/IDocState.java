package perf;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

public interface IDocState {
  Field id();

  Document doc();
}
