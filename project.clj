(defproject elephantdb/elephantdb-cascading "0.0.7"
  :java-source-path "src/jvm"
  :java-fork "true"
  :javac-debug "true"
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [elephantdb "0.0.7"]
                 [cascading/cascading-core "1.2.4"
                  :exclusions [org.codehaus.janino/janino]]]
  :repositories {"conjars" "http://conjars.org/repo"}
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [swank-clojure "1.2.1"]])
