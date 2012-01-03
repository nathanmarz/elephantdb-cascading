(ns elephantdb.cascading.integration-test
  (:use clojure.test
        elephantdb.common.testing
        [clojure.string :only (join)])
  (:require [elephantdb.keyval.testing :as t])
  (:import [cascading.pipe Pipe]
           [cascading.tuple Fields Tuple]
           [cascading.flow FlowConnector]
           [cascading.tap Hfs]
           [elephantdb.persistence JavaBerkDB HashModScheme PersistenceCoordinator]
           [elephantdb DomainSpec Utils]
           [elephantdb.hadoop IdentityUpdater]
           [elephantdb.cascading ElephantDBTap
            ElephantBaseTap$Args ElephantTailAssembly]
           [org.apache.hadoop.io BytesWritable IntWritable]
           [org.apache.hadoop.mapred JobConf]))

(defn hfs-tap [path & fields]
  (-> (Fields. (into-array fields))
      (Hfs. path)))

(defn kv-tap [path]
  (hfs-tap path "key" "value"))

(defn mk-options [& {:keys [updater]}]
  (let [ret (ElephantBaseTap$Args.)]
    (set! (.updater ret) updater)
    ret))

(def props
  {"io.serializations"
   (join "," ["org.apache.hadoop.io.serializer.WritableSerialization"
              "cascading.tuple.hadoop.BytesSerialization"
              "cascading.tuple.hadoop.TupleSerialization"])})

(defn jobconf []
  (let [conf (JobConf.)]
    (doseq [[k v] props]
      (.set conf k v))
    conf))

(defn create-source
  [tmp-path pairs]
  (let [src (kv-tap tmp-path)]
    (with-open [collector (.openForWrite src (jobconf))]
      (doseq [[k v] pairs]
        (.add collector (Tuple. (into-array Object [k v])))))
    src))

(defn connect
  "Connect the supplied source and sink with the supplied pipe."
  [pipe source sink]
  (doto (.connect (FlowConnector. props) source sink pipe)
    (.complete)))

(defn elephant->hfs
  "Transfers all tuples from the supplied elephant-tap into the
  supplied cascading `sink`."
  [elephant-source sink]
  (connect (Pipe. "pipe")
           elephant-source
           sink))

(defn hfs->elephant
  "Transfers all tuples from the supplied cascading `source` to the
  supplied elephant-tap."
  [source elephant-sink]
  (connect (-> (Pipe. "pipe")
               (ElephantTailAssembly. elephant-sink))
           source
           elephant-sink))

(defn fill-domain
  "Fills the supplied elephant-sink with the the supplied sequence of
  kv-pairs."
  [elephant-sink pairs]
  (with-fs-tmp [_ tmp]
    (-> (create-source tmp pairs)
        (hfs->elephant elephant-sink))))

(defn get-tuples
  "Returns all tuples in the supplied cascading tap as a Clojure
  sequence."
  [sink]
  (with-open [it (.openForRead sink (jobconf))]
    (doall (for [wrapper (iterator-seq it)]
             (into [] (.getTuple wrapper))))))

(deftest connect-test
  (are [xs]
       (with-fs-tmp [_ src-tmp sink-tmp]     
         (let [sink (ElephantDBTap. sink-tmp
                                    (DomainSpec. (JavaBerkDB.) (HashModScheme.) 4)
                                    (mk-options))]
           (fill-domain sink xs)))
       [[1 2] [3 4]]
       [["key" "val"] ["ham" "burger"]]))

(defn read-etap-with-flow [path]
  (with-fs-tmp [fs tmp-path]
    (let [source (ElephantDBTap. path)
          sink (kv-tap tmp-path)]
      (elephant->hfs source sink)
      (get-tuples sink))))

(defn check-results
  "TODO: Move over to edb proper."
  [dpath pairs]
  (is (= (set (read-etap-with-flow dpath))
         (set pairs))))

;; TODO: Invalid. Doesn't belong in this project; this makes far too
;; many assumptions about a thrift interface, etc. All we're concerned
;; about here is getting data in and out of edb w/ cascading.
(def-fs-test test-basic [fs tmp]
  (let [spec (DomainSpec. (JavaBerkDB.) (HashModScheme.) 4)
        sink (ElephantDBTap. tmp spec (mk-options :updater nil))
        data [[0 (barr 0 0)]
              [1 (barr 1 1)]
              [2 (barr 2 2)]
              [3 (barr 3 3)]
              [4 (barr 4 4)]
              [5 (barr 5 5)]
              [6 (barr 6 5)]
              [7 (barr 7 5)]
              [8 (barr 8 5)]]
        data2 [[0 (barr 1)
                10 (barr 100)]]]
    (fill-domain sink data)
    (check-results tmp data)
    (fill-domain sink data2)
    (check-results tmp (conj data2 [(barr 1) nil]))))

;; TODO: Invalid. Doesn't belong in this project; this makes far too
;; many assumptions about a thrift interface, etc. All we're concerned
;; about here is getting data in and out of edb w/ cascading.
(def-fs-test test-incremental [fs tmp]
  (let [spec (DomainSpec. (JavaBerkDB.) (HashModScheme.) 2)
        sink (ElephantDBTap. tmp spec (mk-options (IdentityUpdater.)))
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
    (check-results tmp data)
    (fill-domain sink data2)
    (check-results tmp data3)))

(def-fs-test test-source [fs tmp]
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
      (is (t/kv-pairs= pairs (read-etap-with-flow dpath))))))

;; Example of how to do stuff now.
(def spec
  (DomainSpec. (JavaBerkDB.)
               (HashModScheme.)
               4))

(defn populate [idx]
  (with-open [shard (.createShard spec "/Users/sritchie/Desktop/helper" idx)]
    (doseq [x (range 1000)]
      (.index shard (KeyValDocument. x 10)))))

;; or, you can create a domain store directly:
(def store
  (DomainStore. "/Users/sritchie/Desktop/helper"
                (DomainSpec. (JavaBerkDB.)
                             (HashModScheme.)
                             4)))
