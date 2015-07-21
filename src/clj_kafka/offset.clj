(ns ^{:doc "Offset operations."}
  clj-kafka.offset
  (:require [clj-kafka.core :refer [as-properties to-clojure]])
  (:import [kafka.common TopicAndPartition]
           [kafka.javaapi OffsetFetchResponse ConsumerMetadataResponse]
           (kafka.network BlockingChannel)
           (kafka.api RequestOrResponse ConsumerMetadataRequest OffsetFetchRequest OffsetCommitRequest)
           (scala.collection JavaConversions)))

(defn blocking-channel
  "Create a new blocking channel to the Kafka cluster.
  host is the broker server name or IP
  port is the broker server port"
  ([host port]
    (blocking-channel host port nil))
  ([host port {:keys [read-buf-size write-buf-size read-timeout-ms] :as opts
               :or {read-buf-size (BlockingChannel/UseDefaultBufferSize)
                    write-buf-size (BlockingChannel/UseDefaultBufferSize)
                    read-timeout-ms 10000}}]
    (let [channel (BlockingChannel. host port read-buf-size write-buf-size read-timeout-ms)]
        (.connect channel)
         channel)))

(defn consumer-metadata-request
  ([group-id client-id] (consumer-metadata-request group-id 1 client-id))
  ([group-id correlation-id client-id] (ConsumerMetadataRequest. group-id (ConsumerMetadataRequest/CurrentVersion) correlation-id client-id)))

(defn send-channel-message
  [^BlockingChannel channel ^RequestOrResponse message]
  (println message)
  (.send channel message))

(defn find-offset-manager [host port group-id client-id]
  (let [channel-attempt (blocking-channel host port)
        metadata-req (consumer-metadata-request group-id client-id)]
        (send-channel-message channel-attempt metadata-req)
        (let [meta-response (to-clojure (ConsumerMetadataResponse/readFrom (.buffer (.receive channel-attempt))))]
          (println meta-response)
          (if-let [no-error (= (kafka.common.ErrorMapping/NoError) (:error-code meta-response))]
            (if-let [same-channel (and (= host (.host (:coordinator meta-response))) (= port (.port (:coordinator meta-response))))]
              channel-attempt
              (let [coordinator (:coordinator meta-response)
                         new-host (.host coordinator) new-port (.port coordinator)]
                     (println "Different channel" new-host new-port)
                     (.disconnect channel-attempt)
                     (blocking-channel new-host new-port)))
            (throw (RuntimeException. (str meta-response))))
        )))

(defn offset-fetch-request [group-id client-id topic max-partition]
  (let [offset-request-version 1
        correlation-id 1
        topic-partition-java (map (fn [i] (TopicAndPartition. topic i)) (range 0 max-partition))
        topic-partition-scala (.toSeq (JavaConversions/asScalaBuffer topic-partition-java))]
  (OffsetFetchRequest. group-id topic-partition-scala offset-request-version correlation-id client-id)))

(defn fetch-consumer-offsets [offset-manager topic group-id client-id max-partition]
  (let [offset-fetch-req (offset-fetch-request group-id client-id topic max-partition)]
    (send-channel-message offset-manager offset-fetch-req)
    (let [offset-fetch-resp (to-clojure (OffsetFetchResponse/readFrom (.buffer (.receive offset-manager))))]
      (println "Success: " offset-fetch-resp))
    ))

(defn -main [& args]
  (let [group-id "ccm.transaction.consumer.esg-prod.red6_2"
        client-id group-id
        offset-manager (find-offset-manager "kafka002.prod1.connexity.net" 9092 group-id client-id)
        offsets (fetch-consumer-offsets offset-manager "display_prod.transactions.script.synthetic" group-id client-id 4)]
    (println offsets)
    ))