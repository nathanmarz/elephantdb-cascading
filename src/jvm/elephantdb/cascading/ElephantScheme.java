package elephantdb.cascading;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;

public class ElephantScheme extends Scheme {
        @Override
        public void sourceInit(Tap tap, JobConf jc) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void sinkInit(Tap tap, JobConf jc) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Tuple source(Object o, Object o1) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void sink(TupleEntry te, OutputCollector oc) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
