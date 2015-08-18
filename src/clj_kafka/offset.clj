(ns ^{:doc "Offset operations."}
  clj-kafka.offset
  (:require [clojure.tools.logging :as log]
            [clj-kafka.core :refer [as-properties to-clojure]]
            [clj-kafka.consumer.simple :refer [topic-offset consumer]]
            [clj-kafka.zk :refer [partitions]])
  (:import [kafka.common TopicAndPartition OffsetAndMetadata]
           [kafka.javaapi OffsetFetchResponse ConsumerMetadataResponse OffsetCommitResponse]
           (kafka.network BlockingChannel)
           (kafka.api RequestOrResponse ConsumerMetadataRequest OffsetFetchRequest OffsetCommitRequest TopicMetadataRequest)
           (scala.collection JavaConversions)
           (scala.collection.mutable ArrayBuffer)))

(def DEFAULT_CLIENT_ID "clj-kafka-id")

(defn- parse-int [s]
  (Integer. (re-find  #"\d+" s )))

(defn- get-first-broker-config [broker-config]
  (let [first-broker (nth (.split broker-config ",") 0)]
      (.split first-broker ":" 2)))

(defn- blocking-channel
  "Create a new blocking channel to the Kafka cluster.
  host is the broker server name or IP
  port is the broker server port"
  ([broker-config]
    (let [host-port-pair (get-first-broker-config broker-config)]
      (blocking-channel (nth host-port-pair 0) (nth host-port-pair 1))))

  ([host port]
    (blocking-channel host port nil))

  ([host port {:keys [read-buf-size write-buf-size read-timeout-ms] :as opts
               :or {read-buf-size (BlockingChannel/UseDefaultBufferSize)
                    write-buf-size (BlockingChannel/UseDefaultBufferSize)
                    read-timeout-ms 10000}}]
    (let [channel (BlockingChannel. host port read-buf-size write-buf-size read-timeout-ms)]
        (.connect channel)
         channel)))

(defn- send-channel-message
  [^BlockingChannel channel ^RequestOrResponse message]
  (log/debug "Sending channel msg --> " message)
  (.send channel message))

(defn- topic-metadata-request
  ([topic] (topic-metadata-request topic 1 DEFAULT_CLIENT_ID))
  ([topic client-id] (topic-metadata-request topic 1 client-id))
  ([topic correlation-id client-id] (TopicMetadataRequest. (TopicMetadataRequest/CurrentVersion) correlation-id client-id (ArrayBuffer. topic))))

(defn find-topic-partition-count [zk-config topic]
  (let [topic-partitions (partitions {"zookeeper.connect" zk-config}  topic)]
      (.count topic-partitions)))

(defn- consumer-metadata-request
  ([group-id] (consumer-metadata-request group-id 1 DEFAULT_CLIENT_ID))
  ([group-id client-id] (consumer-metadata-request group-id 1 client-id))
  ([group-id correlation-id client-id] (ConsumerMetadataRequest. group-id (ConsumerMetadataRequest/CurrentVersion) correlation-id client-id)))

(defn find-offset-manager
  ([broker-config group-id] (find-offset-manager broker-config group-id DEFAULT_CLIENT_ID))
  ([broker-config group-id client-id]
  (let [host-port-pair (get-first-broker-config broker-config)
        host (nth host-port-pair 0)
        port (parse-int (nth host-port-pair 1))
        channel-attempt (blocking-channel host port)
        metadata-req (consumer-metadata-request group-id client-id)]
        (send-channel-message channel-attempt metadata-req)
        (let [meta-response (to-clojure (ConsumerMetadataResponse/readFrom (.buffer (.receive channel-attempt))))]
          (if-let [no-error (= (kafka.common.ErrorMapping/NoError) (:error-code meta-response))]
            (if-let [same-channel (and (= host (.host (:coordinator meta-response))) (= port (.port (:coordinator meta-response))))]
              channel-attempt
              (let [coordinator (:coordinator meta-response)
                         new-host (.host coordinator) new-port (.port coordinator)]
                     (.disconnect channel-attempt)
                     (blocking-channel new-host new-port)))
            (throw (RuntimeException. (str meta-response))))
        ))))

(defn- offset-fetch-request
  ([group-id topic max-partition] (offset-fetch-request group-id topic max-partition DEFAULT_CLIENT_ID))
  ([group-id topic max-partition client-id]
  (let [offset-request-version 1
        correlation-id 1
        topic-partition-java (map (fn [i] (TopicAndPartition. topic i)) (range 0 max-partition))
        topic-partition-scala (.toSeq (JavaConversions/asScalaBuffer topic-partition-java))]
  (OffsetFetchRequest. group-id topic-partition-scala offset-request-version correlation-id client-id))))

(defn fetch-consumer-offsets
  ([broker-config zk-config topic group-id]
    (let [offset-manager (find-offset-manager broker-config group-id)
          max-partition (find-topic-partition-count zk-config topic)]
      (fetch-consumer-offsets offset-manager topic group-id DEFAULT_CLIENT_ID max-partition)))

  ([offset-manager topic group-id client-id max-partition]
    (let [offset-fetch-req (offset-fetch-request group-id topic max-partition client-id)]
      (send-channel-message offset-manager offset-fetch-req)
      (let [offset-fetch-resp (to-clojure (OffsetFetchResponse/readFrom (.buffer (.receive offset-manager))))]
        (log/debug "fetch-consumer-offsets-response: " offset-fetch-resp)
        offset-fetch-resp)
    )))

(defn- offset-commit-request [group-id client-id topic new-offsets]
  (let [commit-request-version 1
        correlation-id 1
        now (System/currentTimeMillis)
        topic-partition-java (map (fn [i] (TopicAndPartition. topic i)) (range 0 (count new-offsets)))
        topic-partition-offsets (java.util.HashMap. (into {} (for [tp topic-partition-java] [tp (OffsetAndMetadata. (nth new-offsets (.partition tp)) "" now)])))
        emptyMap (JavaConversions/mapAsScalaMap topic-partition-offsets)
        topic-partition-offsets-scala (.$plus$plus (scala.collection.immutable.HashMap.) emptyMap)
        ]
    (OffsetCommitRequest. group-id topic-partition-offsets-scala commit-request-version correlation-id client-id -1 "")))

(defn- try-fecth-topic-offset [single-broker-config topic partition new-offset-type]
  (let [bk-host-port-pair (get-first-broker-config single-broker-config)
        bk-host (nth bk-host-port-pair 0)
        bk-port (parse-int (nth bk-host-port-pair 1))
        consumer (consumer bk-host bk-port DEFAULT_CLIENT_ID)]
    (topic-offset consumer topic partition new-offset-type)))

(defn- fecth-topic-offset [broker-config topic partition new-offset-type]
  (let [brokers (.split broker-config ",")
        potential-offsets (map (fn [single-broker] (try-fecth-topic-offset single-broker topic partition new-offset-type)) brokers)]
    (first (filter #(not (nil? %)) potential-offsets))
    ))

(defn reset-consumer-offsets [broker-config zk-config topic group-id new-offset-type]
   (let [offset-manager (find-offset-manager broker-config group-id)
         max-partition (find-topic-partition-count zk-config topic)
         new-offsets (map (fn [i] (fecth-topic-offset broker-config topic i new-offset-type)) (range 0 max-partition))
         offset-commit-req (offset-commit-request group-id DEFAULT_CLIENT_ID topic new-offsets)
         ]
     (send-channel-message offset-manager offset-commit-req)
     (let [offset-commit-resp (to-clojure (OffsetCommitResponse/readFrom (.buffer (.receive offset-manager))))]
       (log/debug "reset-consumer-offsets-response: " offset-commit-resp)
       offset-commit-resp)))