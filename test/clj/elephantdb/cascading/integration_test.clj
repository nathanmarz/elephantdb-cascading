(ns elephantdb.cascading.integration-test
  (:import [cascading.operation Identity])
  (:import [cascading.pipe Each GroupBy Pipe SubAssembly])
  (:import [cascading.tuple Fields Tuple TupleEntry])
  (:import [cascading.flow FlowConnector])
  (:import [cascading.tap Hfs])
  (:import [elephantdb.persistence JavaBerkDB LocalPersistenceFactory])
  (:import [elephantdb DomainSpec])
  (:import [elephantdb.cascading ElephantDBTap ElephantDBTap$Args ElephantTailAssembly])
  (:import [org.apache.hadoop.io BytesWritable IntWritable])
  (:import [org.apache.hadoop.mapred JobConf])
  (:use [elephantdb testing])
  (:use [clojure test])
  )

(defn create-source [tmppath pairs]
  (let [source (Hfs. (Fields. (into-array ["key" "value"])) tmppath)
        coll (.openForWrite source (JobConf.))]
    (doseq [[k v] pairs]
      (.add coll (Tuple. (into-array Object [(BytesWritable. k) (BytesWritable. v)])))
      )
    (.close coll)
    source ))

(defn emit-to-sink [sink pairs]
  (with-fs-tmp [fs tmp]
    (let [source (create-source tmp pairs)
          p (Pipe. "pipe")
          p (ElephantTailAssembly. p sink)
          flow (.connect (FlowConnector.) source sink p)]
      (.complete flow)
      )))

(defn mk-options [updater]
  (let [ret (ElephantDBTap$Args.)]
    (set! (. ret updater) updater)
    ret
    ))

(defn check-results [dpath pairs]
  (with-single-service-handler [handler {"domain" dpath}]
    (check-domain "domain" handler pairs)
    ))

(deffstest test-basic [fs tmp]
  (let [spec (DomainSpec. (JavaBerkDB.) 4)
        sink (ElephantDBTap. tmp spec (mk-options nil))
        data [[(barr 0) (barr 0 0)]
              [(barr 1) (barr 1 1)]
              [(barr 2) (barr 2 2)]
              [(barr 3) (barr 3 3)]
              [(barr 4) (barr 4 4)]
              [(barr 5) (barr 5 5)]
              [(barr 6) (barr 6 5)]
              [(barr 7) (barr 7 5)]
              [(barr 8) (barr 8 5)]
              ]]
    (emit-to-sink sink data)
    (check-results tmp data)
    ))

(deffstest test-incremental [fs tmp]
  )
