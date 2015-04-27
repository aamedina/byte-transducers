(ns byte-transducers.buffers
  (:require [clojure.core.protocols
             :refer [CollReduce coll-reduce IKVReduce kv-reduce]])
  (:import [java.nio Buffer ByteBuffer DirectByteBuffer CharBuffer]
           [java.nio ShortBuffer IntBuffer LongBuffer]
           [java.nio FloatBuffer DoubleBuffer]))

(set! *warn-on-reflection* true)

(defmacro ^:private def-buffer
  [name class]
  (let [len (with-meta (gensym "len") {:tag 'long})]
    `(defn ~name
       {:arglists '([~'len])
        :tag ~class}
       [~len]
       (.mark (. ~class allocate ~(with-meta len nil))))))

(defmacro ^:private def-direct-buffer
  [name class]
  (let [len (with-meta (gensym "len") {:tag 'long})
        width (case class
                 (ShortBuffer CharBuffer) 2
                 (DoubleBuffer LongBuffer) 8
                 (FloatBuffer IntBuffer) 4
                 nil)
        method (case class
                 CharBuffer 'asCharBuffer
                 DoubleBuffer 'asDoubleBuffer
                 FloatBuffer 'asFloatBuffer
                 IntBuffer 'asIntBuffer
                 LongBuffer 'asLongBuffer
                 ShortBuffer 'asShortBuffer
                 nil)]
    `(defn ~name
       {:arglists '([~'len])
        :tag ~class}
       [~len]
       ~(if (= class 'ByteBuffer)
          `(.mark (ByteBuffer/allocateDirect ~(with-meta len nil)))
          `(let [len# (* ~(with-meta len nil) ~width)]
             (.mark (. (ByteBuffer/allocateDirect len#) ~method)))))))

(def-buffer byte-buffer ByteBuffer)
(def-buffer char-buffer CharBuffer)
(def-buffer short-buffer ShortBuffer)
(def-buffer int-buffer IntBuffer)
(def-buffer long-buffer LongBuffer)
(def-buffer float-buffer FloatBuffer)
(def-buffer double-buffer DoubleBuffer)

(def-direct-buffer direct-byte-buffer ByteBuffer)
(def-direct-buffer direct-char-buffer CharBuffer)
(def-direct-buffer direct-short-buffer ShortBuffer)
(def-direct-buffer direct-int-buffer IntBuffer)
(def-direct-buffer direct-long-buffer LongBuffer)
(def-direct-buffer direct-float-buffer FloatBuffer)
(def-direct-buffer direct-double-buffer DoubleBuffer)

(defmacro ^:private buffer-reduce
  ([buf f] `(buffer-reduce ~buf ~f (~f)))
  ([buf f val]
   `(let [buf# ~(with-meta `(.slice ~buf) (meta buf))]
      (loop [ret# ~val]
        (if (.hasRemaining buf#)
          (let [ret# (~f ret# (.get buf#))]
            (if (reduced? ret#)
              @ret#
              (recur ret#)))
          ret#)))))

(defmacro ^:private buffer-reduce-kv
  ([buf f val]
   `(let [buf# ~(with-meta `(.slice ~buf) (meta buf))]
      (loop [ret# ~val]
        (if (.hasRemaining buf#)
          (let [ret# (~f ret# (.position buf#) (.get buf#))]
            (if (reduced? ret#)
              @ret#
              (recur ret#)))
          ret#)))))

(extend-protocol CollReduce
  ByteBuffer
  (coll-reduce
    ([buf f] (buffer-reduce ^ByteBuffer buf f))
    ([buf f val] (buffer-reduce ^ByteBuffer buf f val)))

  DirectByteBuffer
  (coll-reduce
    ([buf f] (buffer-reduce ^DirectByteBuffer buf f))
    ([buf f val] (buffer-reduce ^DirectByteBuffer buf f val)))

  CharBuffer
  (coll-reduce
    ([buf f] (buffer-reduce ^CharBuffer buf f))
    ([buf f val] (buffer-reduce ^CharBuffer buf f val)))

  DoubleBuffer
  (coll-reduce
    ([buf f] (buffer-reduce ^DoubleBuffer buf f))
    ([buf f val] (buffer-reduce ^DoubleBuffer buf f val)))

  FloatBuffer
  (coll-reduce
    ([buf f] (buffer-reduce ^FloatBuffer buf f))
    ([buf f val] (buffer-reduce ^FloatBuffer buf f val)))

  IntBuffer
  (coll-reduce
    ([buf f] (buffer-reduce ^IntBuffer buf f))
    ([buf f val] (buffer-reduce ^IntBuffer buf f val)))

  LongBuffer
  (coll-reduce
    ([buf f] (buffer-reduce ^LongBuffer buf f))
    ([buf f val] (buffer-reduce ^LongBuffer buf f val)))

  ShortBuffer
  (coll-reduce
    ([buf f] (buffer-reduce ^ShortBuffer buf f))
    ([buf f val] (buffer-reduce ^ShortBuffer buf f val))))

(extend-protocol IKVReduce
  ByteBuffer
  (kv-reduce
    ([buf f val] (buffer-reduce-kv ^ByteBuffer buf f val)))

  DirectByteBuffer
  (kv-reduce
    ([buf f val] (buffer-reduce-kv ^DirectByteBuffer buf f val)))

  CharBuffer
  (kv-reduce
    ([buf f val] (buffer-reduce-kv ^CharBuffer buf f val)))

  DoubleBuffer
  (kv-reduce
    ([buf f val] (buffer-reduce-kv ^DoubleBuffer buf f val)))

  FloatBuffer
  (kv-reduce
    ([buf f val] (buffer-reduce-kv ^FloatBuffer buf f val)))

  IntBuffer
  (kv-reduce
    ([buf f val] (buffer-reduce-kv ^IntBuffer buf f val)))

  LongBuffer
  (kv-reduce
    ([buf f val] (buffer-reduce-kv ^LongBuffer buf f val)))

  ShortBuffer
  (kv-reduce
    ([buf f val] (buffer-reduce-kv ^ShortBuffer buf f val))))

(defprotocol IBuffer
  (put! [buf o]))

(extend-protocol IBuffer
  ByteBuffer
  (put! [buf o] (.put ^ByteBuffer buf (unchecked-byte o)))
  CharBuffer
  (put! [buf o] (.put ^CharBuffer buf (unchecked-char o)))
  ShortBuffer
  (put! [buf o] (.put ^ShortBuffer buf (unchecked-short o)))
  IntBuffer
  (put! [buf o] (.put ^IntBuffer buf (unchecked-int o)))
  LongBuffer
  (put! [buf o] (.put ^LongBuffer buf (unchecked-long o)))
  FloatBuffer
  (put! [buf o] (.put ^FloatBuffer buf (unchecked-float o)))
  DoubleBuffer
  (put! [buf o] (.put ^DoubleBuffer buf (unchecked-double o))))

(defn make-buffer
  [type ^long len]
  (condp identical? type
    Byte/TYPE (byte-buffer len)
    Character/TYPE (char-buffer len)
    Short/TYPE (short-buffer len)
    Integer/TYPE (int-buffer len)
    Long/TYPE (long-buffer len)
    Float/TYPE (float-buffer len)
    Double/TYPE (double-buffer len)))

(defn- from-array
  [type ^objects aseq]
  (let [len (alength aseq)
        buf (make-buffer type len)]
    (areduce aseq idx ret buf
      (put! ret (aget aseq idx)))
    (.flip ^Buffer buf)))

(defonce ^:private array-classes
  {Byte/TYPE (class (byte-array 0))
   Character/TYPE (class (char-array 0))
   Short/TYPE (class (short-array 0))
   Integer/TYPE (class (int-array 0))
   Long/TYPE (class (long-array 0))
   Float/TYPE (class (float-array 0))
   Double/TYPE (class (double-array 0))
   (class (object-array 0)) (class (object-array 0))})

(defn byte-array?
  [o]
  (instance? (get array-classes Byte/TYPE) o))

(defn array?
  [o]
  (some #(instance? % o) (vals array-classes)))
