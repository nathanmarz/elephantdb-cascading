package elephantdb.cascading;

import cascading.tuple.Tuple;
import elephantdb.persistence.Document;

/** User: sritchie Date: 12/16/11 Time: 12:04 AM */
public interface IGateway<D extends Document> {
    public D buildDocument(Tuple tuple);

    public Tuple buildTuple(D document);
}
