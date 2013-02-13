(defproject elephantdb/elephantdb-cascading "0.3.6-SNAPSHOT"
  :min-lein-version "2.0.0"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :jvm-opts ["-server" "-Xmx768m"]
  :repositories {"conjars" "http://conjars.org/repo"}
  :dependencies [[elephantdb "0.2.1-SNAPSHOT"]
                 [cascading/cascading-hadoop "2.0.8"
                  :exclusions [org.codehaus.janino/janino
                               org.apache.hadoop/hadoop-core]]]
  :profiles {:provided
             {:dependencies
              [[org.apache.hadoop/hadoop-core "0.20.2-dev"]]}
             :dev
             {:dependencies
              [[org.clojure/clojure "1.4.0"]
               [hadoop-util "0.2.9"]
               [jackknife "0.1.2"]
               [midje "1.5-alpha9"]]
              :plugins [[lein-midje "3.0-alpha4"]]}})
