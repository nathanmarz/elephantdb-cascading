package elephantdb.cascading;

import cascading.tuple.Tuple;

import java.io.Serializable;

/** User: sritchie Date: 12/16/11 Time: 12:04 AM */
public interface IGateway<D> extends Serializable {
    public D buildDocument(Tuple tuple);
    public Tuple buildTuple(D document);
}