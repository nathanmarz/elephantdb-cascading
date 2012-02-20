(defproject elephantdb/elephantdb-cascading "0.2.0"
  :java-source-path "src/jvm"
  :source-path "src/clj"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"conjars" "http://conjars.org/repo"}
  :dependencies [[elephantdb "0.2.0"]
                 [cascading/cascading-core "1.2.4"
                  :exclusions [org.codehaus.janino/janino]]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [midje "1.3.1" :exclusions [org.clojure/clojure]]
                     [org.clojure/clojure "1.2.1"]
                     [hadoop-util "0.2.7"]
                     [jackknife "0.1.2"]])
