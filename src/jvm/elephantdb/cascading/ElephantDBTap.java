package elephantdb.cascading;

import elephantdb.DomainSpec;

import java.io.IOException;

public class ElephantDBTap extends ElephantBaseTap {
    public ElephantDBTap(String dir, Args args) throws IOException {
        this(dir, null, args);
    }

    public ElephantDBTap(String dir) throws IOException {
        this(dir, null, new Args());
    }

    public ElephantDBTap(String dir, DomainSpec spec) throws IOException {
        this(dir, spec, new Args());
    }

    public ElephantDBTap(String dir, DomainSpec spec, Args args) throws IOException {
        super(dir, spec, args);
    }

    @Override public KeyValGateway freshGateway() {
        return new KeyValGateway();
    }
}
