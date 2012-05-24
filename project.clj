(defproject yieldbot/elephantdb-cascading "0.3.2"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"conjars" "http://conjars.org/repo"}
  :dependencies [[yieldbot/elephantdb "0.2.0-wip4"]
                 [org.slf4j/slf4j-api "1.6.1"]
                 [cascading/cascading-hadoop "2.0.0-wip-281"
                  :exclusions [org.codehaus.janino/janino
                               org.apache.hadoop/hadoop-core]]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [org.clojure/clojure "1.3.0"]
                     [hadoop-util "0.2.8"]
                     [jackknife "0.1.2"]
                     [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [midje "1.3.0"]])
