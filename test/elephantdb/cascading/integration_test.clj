(ns elephantdb.cascading.integration-test
  (:use midje.sweet
        elephantdb.test.common)
  (:require [elephantdb.test.keyval :as t]
            [hadoop-util.test :as test]
            [clojure.string :as s])
  (:import [cascading.pipe Pipe]
           [cascading.tuple Fields Tuple]
           [cascading.flow FlowConnector]
           [cascading.tap Hfs]
           [elephantdb.partition HashModScheme]
           [elephantdb.persistence JavaBerkDB]
           [elephantdb DomainSpec Utils]
           [elephantdb.index IdentityIndexer]
           [elephantdb.document KeyValDocument]
           [elephantdb.store DomainStore]
           [elephantdb.cascading ElephantDBTap
            ElephantDBTap$Args KeyValTailAssembly KeyValGateway]
           [org.apache.hadoop.io BytesWritable IntWritable]
           [org.apache.hadoop.mapred JobConf]))

;; ## Key-Value

(defn kv-spec
  "Returns a DomainSpec initialized with a hash-mod sharding scheme, a
  key-value persistence and the supplied sharding count."
  [shard-count]
  (DomainSpec. (JavaBerkDB.)
               (HashModScheme.)
               shard-count))

(defn hfs-tap
  "Returns an Hfs tap with the default sequencefile scheme and the
  supplied fields."
  [path & fields]
  (-> (Fields. (into-array fields))
      (Hfs. path)))

(defn kv-tap
  "Returns an HFS SequenceFile tap with two fields for key and
  value."
  [path]
  (hfs-tap path "key" "value"))

(defn kv-opts
  "Returns an EDB argument object tuned"
  [& {:keys [indexer recompute?]}]
  (let [ret (ElephantDBTap$Args.)]
    (set! (.gateway ret) (KeyValGateway.))
    (when indexer
      (set! (.indexer ret) indexer))
    (when recompute?
      (set! (.recompute ret) recompute?))
    ret))

(def defaults
  {"io.serializations"
   (s/join "," ["org.apache.hadoop.io.serializer.WritableSerialization"
                "cascading.tuple.hadoop.BytesSerialization"
                "cascading.tuple.hadoop.TupleSerialization"])})

(def mk-props
  (partial merge defaults))

(defn job-conf
  "Returns a JobConf instance, optionally augmented by the supplied
   property map."
  ([] (job-conf {}))
  ([prop-map]
     (let [conf (JobConf.)]
       (doseq [[k v] (mk-props prop-map)]
         (.set conf k v))
       conf)))

(defn flow-connector
  "Returns an instance of FlowConnection, optionally augmented by the
   supplied property map."
  ([] (flow-connector {}))
  ([prop-map]
     (FlowConnector. (mk-props prop-map))))

(defn conj-serialization!
  "Appends the supplied serialization to the supplied JobConf
  object. Returns the modified JobConf object."
  [conf serialization]
  (let [old-val (.get conf "io.serializations")
        new-val (str old-val "," serialization)]
    (.set conf "io.serializations" new-val)))

(defn populate!
  "Accepts a key-value tap, a sequence of key-value pairs (and an
  optional JobConf instance, supplied with the :conf keyword argument)
  and sinks all key-value pairs into the tap. Returns the original tap
  instance.."
  [kv-tap kv-pairs & {:keys [props]}]
  (with-open [collector (.openForWrite kv-tap (job-conf props))]
    (doseq [[k v] kv-pairs]
      (.add collector (Tuple. (into-array Object [k v])))))
  kv-tap)

(defn connect!
  "Connect the supplied source and sink with the supplied pipe."
  [pipe source sink & {:keys [props]}]
  (doto (.connect (flow-connector props) source sink pipe)
    (.complete)))

(defn tuple-seq
  "Returns all tuples in the supplied cascading tap as a Clojure
  sequence."
  [sink]
  (with-open [it (.openForRead sink (job-conf))]
    (doall (for [wrapper (iterator-seq it)]
             (into [] (.getTuple wrapper))))))

;; ## Transfer Functions

(defn elephant->hfs!
  "Transfers all tuples from the supplied elephant-tap into the
  supplied cascading `sink`."
  [elephant-source sink]
  (connect! (Pipe. "pipe")
            elephant-source
            sink))

(defn hfs->elephant!
  "Transfers all tuples from the supplied cascading `source` to the
  supplied elephant-tap."
  [source elephant-sink]
  (connect! (-> (Pipe. "pipe")
                (KeyValTailAssembly. elephant-sink))
            source
            elephant-sink))

(defn fill-domain
  "Fills the supplied elephant-sink with the the supplied sequence of
  kv-pairs."
  [elephant-sink pairs]
  (test/with-fs-tmp [_ tmp]
    (-> (kv-tap tmp)
        (populate! pairs)
        (hfs->elephant! elephant-sink))))

(defn read-etap-with-flow [tap]
  (test/with-fs-tmp [fs tmp-path]
    (let [sink (kv-tap tmp-path)]
      (elephant->hfs! source sink)
      (tuple-seq sink))))

(defn just-tuples [actual]
  (chatty-checker [expected]
                  (= (set (tuple-seq actual))
                     (set expected))))

(tabular
 (fact "connect-test"
   (test/with-fs-tmp [_ src-tmp sink-tmp]     
     (let [sink (ElephantBaseTap. sink-tmp
                                  (kv-spec 4)
                                  (mk-options))]
       (fill-domain sink ?xs))))
 ?xs
 [[1 2] [3 4]]
 [["key" "val"] ["ham" "burger"]])

;; TODO: Invalid. Doesn't belong in this project; this makes far too
;; many assumptions about a thrift interface, etc. All we're concerned
;; about here is getting data in and out of edb w/ cascading.
(fact "test basic"
  (test/with-fs-tmp [fs tmp]
    (let [spec (DomainSpec. (JavaBerkDB.) (HashModScheme.) 4)
          sink (ElephantBaseTap. tmp spec (mk-options :indexer nil))
          data {0 (barr 0 0)
                1 (barr 1 1)
                2 (barr 2 2)
                3 (barr 3 3)
                4 (barr 4 4)
                5 (barr 5 5)
                6 (barr 6 5)
                7 (barr 7 5)
                8 (barr 8 5)}
          data2 {0 (barr 1)
                 10 (barr 100)}]
      (fill-domain sink data)
      (tuple-seq sink) => (just (seq data) :in-any-order)
      (fill-domain sink data2)
      (tuple-seq sink) => (just (conj (seq data2) [(barr 1) nil]) :in-any-order))))

;; TODO: Invalid. Doesn't belong in this project; this makes far too
;; many assumptions about a thrift interface, etc. All we're concerned
;; about here is getting data in and out of edb w/ cascading.
(fact "test-incremental"
  (test/with-fs-tmp [fs tmp]
    (let [spec (DomainSpec. (JavaBerkDB.) (HashModScheme.) 2)
          sink (ElephantBaseTap. tmp spec (mk-options :indexer (IdentityIndexer.)))
          data [[(barr 0) (barr 0 0)]
                [(barr 1) (barr 1 1)]
                [(barr 2) (barr 2 2)]]
          data2 [[(barr 0) (barr 1)]
                 [(barr 3) (barr 3)]]
          data3 [[(barr 0) (barr 1)]
                 [(barr 1) (barr 1 1)]
                 [(barr 2) (barr 2 2)]
                 [(barr 3) (barr 3)]]]
      (fill-domain sink data)
      (tuple-seq sink) => (just (seq data) :in-any-order)
      (fill-domain sink data2)
      (tuple-seq sink) => (just (seq data3) :in-any-order))))

(fact "test-source"
  (test/with-fs-tmp [fs tmp]
    (let [pairs [[(barr 0) (barr 0 2)]
                 [(barr 1) (barr 1 1)]
                 [(barr 2) (barr 9 1)]
                 [(barr 33) (barr 0 2 3)]
                 [(barr 4) (barr 0)]
                 [(barr 5) (barr 1)]
                 [(barr 6) (barr 3)]
                 [(barr 7) (barr 9 101 9 9)]
                 [(barr 81) (barr 9 9 9 1)]
                 [(barr 9) (barr 9 9 2)]
                 [(barr 102) (barr 3 6)]]]
      (t/with-sharded-domain [dpath
                              {:num-shards 6
                               :persistence-factory (JavaBerkDB.)}
                              pairs]
        (fact
          (t/kv-pairs= pairs (read-etap-with-flow dpath)) => true)))))

;; Example of how to do stuff now.

(defn populate [root idx]
  (with-open [shard (.createShard (kv-spec 4) root idx)]
    (doseq [x (range 1000)]
      (.index shard (KeyValDocument. x 10)))))

;; or, you can create a domain store directly:

(defn store []
  (test/with-fs-tmp [_ tmp]
    (DomainStore. tmp (DomainSpec. (JavaBerkDB.)
                                   (HashModScheme.)
                                   4))))
