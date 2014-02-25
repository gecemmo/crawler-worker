(ns crawler-worker.core
  (:require [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt! go close!]])
  (:import [java.nio ByteBuffer]
           [java.nio.file OpenOption StandardOpenOption]
           [java.nio ByteBuffer]
           [java.nio.file Paths Files FileSystems]
           [java.util.concurrent TimeUnit Executors]
           [java.nio.channels CompletionHandler
            AsynchronousSocketChannel
            AsynchronousFileChannel
            AsynchronousChannelGroup
            AsynchronousServerSocketChannel]
           [java.nio.channels.spi
            AsynchronousChannelProvider])
   (:require [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.exchange  :as le]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb]))

(def rabbit-conn (rmq/connect))
(def rabbit-ch (lch/open rabbit-conn))
(def quote-exchange "stocks")

;; nio completion handlers

(defmacro with-handlers [cbody fbody]
  `(reify CompletionHandler
     (completed [t# r# a#]
       ((~@cbody) t# r# a#))
     (failed [_ e# _]
       (do (if (instance? java.nio.channels.InterruptedByTimeoutException e#)
               (println "! Timeout (read), closing.")
               (println "! Failed (read):" e#))
           ((~@fbody))))))

(defmacro with-handler [cbody]
  `(with-handlers (~@cbody) (fn [])))

(defn byte-buf [s]
  (letfn [(new-buf [size]
            (ByteBuffer/allocateDirect size))]
    (cond (instance? String s)
          (-> (new-buf (.length s))
              (.put (.getBytes s))
              .rewind)
          (instance? Number s) (new-buf s)
          true (new-buf 1024))))


(defn channel-group []
  (let [executor (Executors/newFixedThreadPool 8)]
       (AsynchronousChannelGroup/withThreadPool executor)))


(defn normalize-url [base url]
  (cond (re-find #"http" url)  url
        (re-find #"mailto" url) url
        (re-find #"www" url) (str "http://" url)
        true (str "http://" base (if (not (= (first url) \/)) "/") url)))


;; add urls to hash
(def url-count (agent 0))

@url-count

(def url-channel (chan))

(defn read-and-send []
  (async/go-loop [buff (java.lang.StringBuilder. (* 256 1024))]
                 (when-let [in (<! url-channel)]
                   (.append buff in)
                   (.append buff "\n")
                   (println "## SIZE: " (.length buff))
                   (if (> (.length buff) (* 1 1024))
                     (do
                       (lb/publish rabbit-ch quote-exchange "discovered-urls" (.toString buff) :content-type "text/plain" :type "quote.update")
                       (recur (java.lang.StringBuilder.)))
                     (recur buff)))))

(read-and-send)


(comment
(defn ach-reader [ch base-url sch]
  "a channel reader that will close on channel close"
  (async/go-loop [buff (java.lang.StringBuilder.)]
    (when-let [in (<! ch)]
      ;(println (String. in))
      (doseq [s (re-seq  #"(?i)<a href=[\"\']([^>^\"\']*)" (String. in))]
        (>! url-channel (normalize-url base-url (second s)))
        ;(send url-count inc)
        ;(.append buff (normalize-url base-url (second s)))
        ;(.append buff "\n")
        (println "*** NEW-URL: " (normalize-url base-url (second s)))))
      (recur buff))
    ;(println "TODO: send to MQ: " buff)
    ;(lb/publish rabbit-ch quote-exchange "discovered-urls" (.toString buff) :content-type "text/plain" :type "quote.update")
  (.close sch)
  (println "Closing..." (.toString (java.util.Date.)) @url-count))
)

(defn ach-reader [ch base-url sch]
  "a channel reader that will close on channel close"
  (async/go-loop []
    (when-let [in (<! ch)]
      (doseq [s (re-seq  #"(?i)<a href=[\"\']([^>^\"\']*)" (String. in))]
        (>! url-channel (normalize-url base-url (second s)))
        (send url-count inc)
        (println "*** NEW: " (normalize-url base-url (second s))))
      (recur))
    (.close sch)
    (println "Closing..." (.toString (java.util.Date.)) @url-count)))

(defn read-buf
  [^ByteBuffer buf cnt]
    (let [bytes (byte-array cnt)]
      (.flip buf)
      (.get buf bytes)
      (.clear buf)
      bytes))

(comment
(defn read-sock-ch
  "reads a socket channel using core.async channels"
  [^AsynchronousSocketChannel sch ach]
  (let [buf (byte-buf 1024)
        close (fn []
                (println "CLOSING!!!")
                (async/close! ach)
                (.close sch))]
    (.read sch buf nil
           (with-handlers
             (fn [t cnt a]
               (println "COUNT: " cnt)
               (if (neg? cnt) (close)
                   (when-let [bytes (read-buf buf cnt)]
                     ;(go (>! ach bytes))
                     (if (re-find #"</html>" (String. bytes)) (.close sch)
                         (.read sch buf nil t)))))
             (fn [] (close)))) ach))
)

(defn read-sock-ch
  "reads a socket channel using core.async channels"
  [^AsynchronousSocketChannel sch ach]
  (let [buf (byte-buf 1024)
        close (fn []
                (println "CLOSING!!!")
                (async/close! ach)
                (.close sch))]
    (.read sch buf 5 TimeUnit/SECONDS nil
           (with-handlers
             (fn [t cnt a]
               (if (neg? cnt) (close)
               (when-let [bytes (read-buf buf cnt)]
                   (go (>! ach bytes))
                   (.read sch buf 5 TimeUnit/SECONDS nil t))))
             (fn [] (close)))) ach))

(defn write-sock-ch
  "writes a byte array to a socket channel"
  [^AsynchronousSocketChannel sch str]
  (let [buf (byte-buf str)]
    (.write sch buf nil
            (with-handlers
              (fn [& _])
              (fn [& _] (.close sch))))))

(def cg (channel-group))

(defn crawl-url [^java.net.URL url]
  (let [ach (chan)
        host (.getHost url)
        reqstr  (str "GET " (if (empty? (.getPath url)) "/" (.getPath url)) " HTTP/1.1\r\nHost:" host "\r\nConnection: close\r\n\r\n")
        client (AsynchronousSocketChannel/open cg)
        _ (ach-reader ach (.getHost url) client)]
    (try
      (.connect client (java.net.InetSocketAddress. (.getHost url) 80)
                nil (with-handler
                      (fn [& _]
                        (println "connected..., sending request: " reqstr)
                        (write-sock-ch client reqstr)
                        (read-sock-ch client ach))))
      (catch java.nio.channels.UnresolvedAddressException e (println (.getMessage e))))))

;;;;;;;;;;;;;; RABBIT MQ PART

;; function that converts a rabbit-mq topic to a core.async channel
(defn create-topic-channel-reader
  [ch ach queue-name routing-key]
  (let [queue-name' (.getQueue (lq/declare ch queue-name :exclusive false :auto-delete true))
        handler     (fn [ch {:keys [routing-key] :as meta} ^bytes payload]
                      (go (>! ach (String. payload "UTF-8")))
                      (println (format "[consumer] Consumed '%s' from %s, routing key: %s"
                                       (String. payload "UTF-8") queue-name' routing-key)))]
    (lq/bind    ch queue-name' quote-exchange :routing-key routing-key)
    (lc/subscribe ch queue-name' handler :auto-ack true) ach))

;; reads a channel
(defn ach-mq-reader [ch]
  "a channel reader that will close on channel close"
  (async/go-loop []
                 (when-let [in (<! ch)]
                   (println "*** NEW: " in)

                   (try
                     (crawl-url (java.net.URL. in))
                     (catch java.net.MalformedURLException e (println (.getMessage e))))
                   (recur))
                 (println "Closing..." (.toString (java.util.Date.)))))

(defn -main
  [& args]
  (let [;conn      (rmq/connect)
        ;ch        (lch/open conn)
        ach       (chan)]
    (le/declare rabbit-ch quote-exchange "topic" :durable false :auto-delete true)
    ;(le/declare rabbit-ch quote-exchange "discovered-urls" :durable false :auto-delete true)
    (create-topic-channel-reader rabbit-ch ach "topic" "to-crawl-urls")
    (ach-mq-reader ach)))
