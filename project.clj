(defproject elephantdb/elephantdb-cascading "0.2.0"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"conjars" "http://conjars.org/repo"}
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [elephantdb "0.2.0"]
                 [cascading/cascading-core "1.2.4"
                  :exclusions [org.codehaus.janino/janino]]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]])
