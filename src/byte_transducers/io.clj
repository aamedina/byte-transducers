(ns byte-transducers.io
  (:require [clojure.java.io :as io])
  (:import [java.io File RandomAccessFile]
           [java.nio.channels FileChannel FileChannel$MapMode]
           [java.nio MappedByteBuffer]))

(set! *warn-on-reflection* true)

(defn ^RandomAccessFile random-access-file
  "Given an arg which can be coerced into a file via clojure.java.io/as-file
  and an access mode, returns a java.io.RandomAccessFile."
  ([x] (random-access-file x "r"))
  ([x ^String mode] (RandomAccessFile. (io/as-file x) mode)))

(defn- map-mode->access-mode
  [mode]
  (case mode
    (:private :read-only) "r"
    :read-write "rw"
    "r"))

(defn- file-channel-map-mode
  [mode]
  (case mode
    :private FileChannel$MapMode/PRIVATE
    :read-only FileChannel$MapMode/READ_ONLY
    :read-write FileChannel$MapMode/READ_WRITE
    mode))

(defn ^MappedByteBuffer mapped-byte-buffer
  ([x] (mapped-byte-buffer x :read-only))
  ([x mode] (mapped-byte-buffer x mode 0))
  ([x mode pos] (mapped-byte-buffer x mode pos nil))
  ([x mode pos len]
   (let [map-mode (file-channel-map-mode mode)
         access-mode (map-mode->access-mode mode)]
     (with-open [raf (random-access-file x access-mode)
                 ch (.getChannel raf)]
       (.map ch map-mode pos (or len (.length raf)))))))
