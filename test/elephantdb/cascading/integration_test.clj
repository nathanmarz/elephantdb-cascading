(ns elephantdb.cascading.integration-test
  (:use midje.sweet
        clojure.test
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
           [elephantdb.index StringAppendIndexer]
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

(defn elephant-tap
  "Returns an ElephantDB Tap tuned to the supplied path and
  shard-count. Optionally, you can supply keyword arguments as
  specified by `kv-opts`."
  [path shard-count & options]
  (ElephantDBTap. path
                  (kv-spec shard-count)
                  (apply kv-opts options)))

(defn populate!
  "Accepts a SequenceFile tap, a sequence of key-value pairs (and an
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

(defn populate-edb!
  "Fills the supplied elephant-sink with the the supplied sequence of
  kv-pairs."
  [elephant-sink pairs]
  (test/with-fs-tmp [_ tmp]
    (-> (kv-tap tmp)
        (populate! pairs)
        (hfs->elephant! elephant-sink))))

(defn produces
  "Returns a chatty checker that tests for equality between two
  sequences of tuples."
  [expected]
  (chatty-checker [actual]
                  (= (set expected)
                     (set (tuple-seq actual)))))

;; ## Tests

(deftest round-trip-test
  (tabular
   (fact
     "Tuples sunk into an ElephantDB tap and read back out should
    match. (A map acts as a sequence of 2-tuples, perfect for
    ElephantDB key-val tests.)"
     (test/with-fs-tmp [_ tmp]
       (let [e-tap (elephant-tap tmp 4)]
         (populate-edb! e-tap ?tuples)
         e-tap => (produces ?tuples))))
   ?tuples
   {1 2, 3 4}
   {"key" "val", "ham" "burger"}))

(deftest recompute-test
  (fact "FILLIN"
    (test/with-fs-tmp [fs tmp]
      (let [sink (elephant-tap tmp 4 :recompute? true)
            data {0 "zero"
                  1 "one"
                  2 "two"
                  3 "three"
                  4 "four"
                  5 "five"
                  6 "six"
                  7 "seven"
                  8 "eight"}
            data2 {0 "zero!!"
                   10 "ten"}]
        (fact "FILLIN"
          (populate-edb! sink data)
          sink => (produces data))

        (fact "FILLIN"
          (populate-edb! sink data2)
          sink => (produces (merge data2 {0 nil})))))))

(deftest replace-test
  (facts "FILLIN"
    (test/with-fs-tmp [fs tmp]
      (let [sink (elephant-tap tmp 2)
            data {0 "zero"
                  1 "one"
                  2 "two"}
            data2 {0 "ZERO!"
                   3 "THREE!"}]
        (fact "FILLIN"
          (populate-edb! sink data)
          sink => (produces data))

        (fact "FILLIN"
          (populate-edb! sink data2)
          sink => (produces (merge data data2)))))))

(deftest incremental-test
  (facts "FILLIN"
    (test/with-fs-tmp [fs tmp]
      (let [sink (elephant-tap tmp 2 :indexer (StringAppendIndexer.))
            data   {0 "zero"
                    1 "one"
                    2 "two"}
            data2  {0 "ZERO!"
                    2 "TWO!"
                    3 "THREE!"}
            merged {0 "zeroZERO!"
                    1 "one"
                    2 "twoTWO!"
                    3 "THREE!"}]
        (fact "FILLIN"
          (populate-edb! sink data)
          sink => (produces data))
        
        (fact "FILLIN"
          (populate-edb! sink data2)
          sink => (produces merged))))))
