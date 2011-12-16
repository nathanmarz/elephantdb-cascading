package elephantdb.cascading;

import cascading.tuple.Tuple;
import elephantdb.persistence.Document;

/** User: sritchie Date: 12/16/11 Time: 12:12 AM */
public class IdentityGateway implements IGateway {
    public Document buildDocument(Tuple tuple) {
        return (Document) tuple.getObject(0);
    }

    public Tuple buildTuple(Document document) {
        return new Tuple(document);
    }
}
