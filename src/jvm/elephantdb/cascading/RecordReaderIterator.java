package elephantdb.cascading;

import cascading.util.SingleValueIterator;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

class RecordReaderIterator extends SingleValueIterator<RecordReader> {

    public RecordReaderIterator(RecordReader input) {
        super(input);
    }

    @Override public void close() throws IOException {
        getCloseableInput().close();
    }
}
