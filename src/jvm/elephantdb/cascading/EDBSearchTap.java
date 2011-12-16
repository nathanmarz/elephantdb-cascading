package elephantdb.cascading;

import elephantdb.DomainSpec;

import java.io.IOException;

/** User: sritchie Date: 11/22/11 Time: 5:58 PM */
public class EDBSearchTap extends ElephantBaseTap<IdentityGateway> {

    // TODO: What constructors make sense? Check wonderdog.
    public EDBSearchTap(String dir, Args args) throws IOException {
        this(dir, null, args);
    }

    public EDBSearchTap(String dir) throws IOException {
        this(dir, null, new Args());
    }

    public EDBSearchTap(String dir, DomainSpec spec) throws IOException {
        this(dir, spec, new Args());
    }

    public EDBSearchTap(String dir, DomainSpec spec, Args args) throws IOException {
        super(dir, spec, args);
    }

    @Override public IdentityGateway freshGateway() {
        return new IdentityGateway();
    }
}

