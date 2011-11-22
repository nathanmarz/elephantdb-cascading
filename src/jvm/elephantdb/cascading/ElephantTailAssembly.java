package elephantdb.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import elephantdb.persistence.LocalPersistenceFactory;
import org.apache.hadoop.io.BytesWritable;

import java.util.UUID;


public class ElephantTailAssembly extends SubAssembly {
    public static class Shardize extends BaseOperation implements Function {
        int _numShards;
        LocalPersistenceFactory _fact;

        // Pass in an object that implements the KeySharder interface.
        public Shardize(String outfield, int numShards, LocalPersistenceFactory fact) {
            super(new Fields(outfield));
            _fact = fact;
            _numShards = numShards;
        }

        public void operate(FlowProcess process, FunctionCall call) {
            Object key = call.getArguments().get(0);
            Object val = call.getArguments().get(1);

            byte[] serkey = _fact.getTransmitter().serializeKey(key);
            int shard = _fact.getSharder().shardIndex(_numShards, key, val);
            call.getOutputCollector().add(new Tuple(shard));
        }
    }

    public static class MakeSortableKey extends BaseOperation implements Function {
        LocalPersistenceFactory _fact;

        public MakeSortableKey(String outfield, LocalPersistenceFactory fact) {
            super(new Fields(outfield));
            _fact = fact;
        }

        public void operate(FlowProcess process, FunctionCall call) {
            Object key = call.getArguments().get(0);
            byte[] serkey = _fact.getTransmitter().serializeKey(key);
            byte[] sortkey = _fact.getKeySorter().getSortableKey(serkey);

            call.getOutputCollector().add(new Tuple(new BytesWritable(sortkey)));
        }
    }

    public ElephantTailAssembly(Pipe keyValuePairs, ElephantDBTap outTap) {

        // generate two random field names
        String shardfield = "shard" + UUID.randomUUID().toString();
        String keysortfield = "keysort" + UUID.randomUUID().toString();

        LocalPersistenceFactory lp = outTap.getSpec().getLPFactory();
        int numShards = outTap.getSpec().getNumShards();
        // shardize the key.
        Pipe out = new Each(keyValuePairs, new Fields(0), new Shardize(shardfield, numShards, lp), Fields.ALL);
        out = new Each(out, new Fields(0), new MakeSortableKey(keysortfield, lp), Fields.ALL);

        //put in order of shard, key, value, sortablekey
        out = new Each(out, new Fields(2, 0, 1, 3), new Identity(), Fields.RESULTS);
        out = new GroupBy(out, new Fields(0), new Fields(3)); // group by shard

        // emit shard, key, value
        out = new Each(out, new Fields(0, 1, 2), new Identity());
        setTails(out);
    }
}
