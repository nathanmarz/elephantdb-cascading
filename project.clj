(defproject elephantdb/elephantdb-cascading "0.3.5-SNAPSHOT"
  :min-lein-version "2.0.0"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :jvm-opts ["-server" "-Xmx768m"]
  :repositories {"conjars" "http://conjars.org/repo"}
  :dependencies [[yieldbot/elephantdb "0.2.0-SNAPSHOT"]
                 [cascading/cascading-hadoop "2.0.6"
                  :exclusions [org.apache.hadoop/hadoop-core]]]
  :profiles {:provided
             {:dependencies
              [[org.apache.hadoop/hadoop-core "0.20.2-dev"]]}
             :dev
             {:dependencies
              [[org.clojure/clojure "1.4.0"]
               [hadoop-util "0.2.8"]
               [jackknife "0.1.2"]
               [midje "1.4.0"
                :exclusions [org.clojure/clojure]]]
              :plugins [[lein-midje "2.0.3"]]}})
