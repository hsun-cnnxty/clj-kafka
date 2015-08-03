(ns clj-kafka.test.producer
  (:use [expectations]
        [clj-kafka.offset])
  (:import [kafka.producer KeyedMessage]))

(expect 10 (parse-int "10"))


