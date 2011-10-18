(defproject elephantdb/elephantdb-cascading "0.1.0"
  :java-source-path "src/jvm"
  :test-path "test/clj"
  :java-fork "true"
  :javac-debug "true"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [elephantdb "0.1.0"]
                 [cascading/cascading-core "1.2.4"
                  :exclusions [org.codehaus.janino/janino]]]
  :repositories {"conjars" "http://conjars.org/repo"}
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [swank-clojure "1.4.0-SNAPSHOT"]])
