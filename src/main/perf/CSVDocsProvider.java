package perf;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.util.BytesRef;

public class CSVDocsProvider implements DocsProvider {

  private final static int BUFFER_SIZE = 1 << 16;
  private final static Record END = new Record(List.of());
  private final BlockingQueue<Record> queue = new ArrayBlockingQueue<>(1024);
  private final AtomicLong bytesIndexed = new AtomicLong();
  private final AtomicInteger index = new AtomicInteger();

  private BufferedReader reader;
  private List<HeaderItem> header;


  public CSVDocsProvider(String path) {
    Thread readerThread = new DocsProvider.ReaderThread<>(queue, END, () -> this.parseCSVData(path));
    readerThread.setName("CSVDocsProvider reader");
    readerThread.setDaemon(true);
    readerThread.start();
  }

  @Override
  public long getBytesIndexed() {
    return this.bytesIndexed.get();
  }

  public IDocState newDocState() {
    return new CSVDocState();
  }


  @Override
  public Document nextDoc(IDocState idocState, boolean expected) throws IOException {
    if (!(idocState instanceof CSVDocState)) {
      throw new RuntimeException("doc must be an instance of DocState");
    }
    CSVDocState docState = (CSVDocState) idocState;

    Record record;
    try {
      record = queue.take();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }
    if (record == END) {
      return null;
    }



    if (record.fields.size() != header.size()) {
      throw new RuntimeException("Invalid number of fields in record");
    }

    Document doc = new Document();
    for (int i = 0; i < record.fields.size(); i++) {
      HeaderItem headerItem = header.get(i);
      String field = record.fields.get(i).trim();
      if (field.isEmpty()) {
        continue;
      }

      switch (headerItem.getType()){
        case "string":
          doc.add(new StringField(headerItem.name, field, Field.Store.NO));
          doc.add(new SortedSetDocValuesField(headerItem.name, new BytesRef(field)));
          this.bytesIndexed.addAndGet(field.length());
          break;
        case "long":
          try {
            int intValue = Integer.parseInt(field);
            doc.add(new LongPoint(headerItem.name, intValue));
            doc.add(new NumericDocValuesField(headerItem.name, intValue));
            this.bytesIndexed.addAndGet(4);
          } catch (NumberFormatException e1) {
            // do nothing
          }
          break;
        default:
          throw new RuntimeException("Unsupported field type: " + headerItem.getType());
      }
    }


    int id = index.getAndIncrement();
    docState.id.setStringValue(LineFileDocs.intToID(id));

    doc.add(docState.id);
    docState.doc = doc;
    return docState.doc;
  }


  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }


  public void parseCSVData(String path) throws Exception {
    this.reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8), BUFFER_SIZE);
    this.header = parseHeader(reader.readLine());

    String line;
    while ((line = reader.readLine()) != null) {
      Record record = parseLine(line);
      this.queue.put(record);
    }
  }


  private static List<HeaderItem> parseHeader(String headerLine) {
    List<HeaderItem> headerItems = new ArrayList<>();

    // matches "Name (Type)" pattern
    String regex = "(.+?)\\s*\\((.+?)\\)";
    Pattern pattern = Pattern.compile(regex);

    String[] items = headerLine.split(",");
    for (String item : items) {
      item = item.trim();
      Matcher matcher = pattern.matcher(item);

      if (matcher.matches()) {
        String name = matcher.group(1).trim();
        String type = matcher.group(2).trim();
        headerItems.add(new HeaderItem(name, type));
      } else {
        throw new IllegalArgumentException("Header format is incorrect for item: " + item);
      }
    }

    return headerItems;
  }

  private static Record parseLine(String line) {
    List<String> fields = new ArrayList<>();
    StringBuilder currentField = new StringBuilder();
    boolean insideQuotes = false;

    for (char c : line.toCharArray()) {
      if (c == '"') {
        insideQuotes = !insideQuotes; // Toggle insideQuotes flag
      } else if (c == ',' && !insideQuotes) {
        // Add field if comma is outside quotes
        fields.add(currentField.toString());
        currentField.setLength(0); // Clear the builder
      } else {
        // Append character to the current field
        currentField.append(c);
      }
    }
    fields.add(currentField.toString()); // Add the last field
    return new Record(fields);
  }


  private static class HeaderItem {

    private final String name;
    private final String type;

    public HeaderItem(String name, String type) {
      this.name = name;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }
  }

  private static class Record {
    private final List<String> fields;

    private Record(List<String> fields) {
      this.fields = fields;
    }
  }

  private static class CSVDocState implements IDocState {
    private final Field id = new StringField("id", "", Field.Store.YES);
    private Document doc = new Document();

    @Override
    public Field id() {
      return id;
    }

    @Override
    public Document doc() {
      return doc;
    }
  }
}
