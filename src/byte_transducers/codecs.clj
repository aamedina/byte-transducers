(ns byte-transducers.codecs
  (:refer-clojure :exclude [identity])
  (:require [byte-transducers.buffers :as buffers]
            [byte-transducers.generic :as xf]
            [byte-transducers.io :as io]
            [clojure.walk :as walk :refer [postwalk-replace]])
  (:import [java.nio Buffer ByteBuffer ByteOrder]
           [java.nio.charset Charset CharsetEncoder CharsetDecoder]
           [java.nio.charset CoderResult]))

(set! *warn-on-reflection* true)

(set! *unchecked-math* :warn-on-boxed)

(defmacro def-primitive-codec
  "Defines a function of three arities suitable for encoding and decoding.

   sizeof - (arity 0) - returns the number of bytes this codec reads and writes.
   decode - (arity 1) - given a ByteBuffer, reads a single value.
   encode - (arity 2) - writes the given value to the given ByteBuffer.
   
   This macro uses a tiny domain specific language which allows extenders to 
   succinctly define additional primitive codecs if they feel the need to
   do so in their applications.

   name - the name of the defn.
   sizeof - body of the sizeof method described above.
   getter - either a dotted symbol (signifying an instance method or a form 
            which binds the ByteBuffer to % ala the #() reader macro.
   setter - either a dotted symbol (signifying an instance method) or a form 
            which binds the ByteBuffer to %1 and the value to encode to %2.
   :pre-encoder - optional form used to transform the value before it gets
                  encoded into the buffer.
   :post-decoder - optional form used to transform the decoded value from the 
                   input buffer."
  [name sizeof getter setter & {:keys [pre-encoder post-decoder]}]
  (let [tag (:tag (meta name))
        name (with-meta name (dissoc (meta name) :tag))
        m (merge `{:arglists '([] [~'buf] [~'buf ~'val])} (meta name))
        buf (with-meta (gensym "buf") {:tag 'java.nio.ByteBuffer})
        val (gensym "val")
        ret (gensym "ret")
        getter (cond
                 (coll? getter) (with-meta (postwalk-replace {'% buf} getter)
                                  (meta getter))
                 getter (list getter buf)
                 :else buf)
        setter (cond
                 (coll? setter) (with-meta (postwalk-replace {'%1 buf '%2 ret}
                                                             setter)
                                  (meta setter))
                 setter (list setter buf ret)
                 :else buf)
        post-decoder (cond
                       (coll? post-decoder)
                       (with-meta (postwalk-replace {'%1 buf '%2 ret}
                                                    post-decoder)
                         (meta post-decoder))
                       post-decoder (list post-decoder ret)
                       :else ret)
        pre-encoder (cond
                      (coll? pre-encoder)
                      (with-meta (postwalk-replace {'%1 buf '%2 val}
                                                   pre-encoder)
                        (meta pre-encoder))
                      pre-encoder (list pre-encoder val)
                      :else val)
        name-be (with-meta (symbol (str name "-be")) (meta name))
        name-le (with-meta (symbol (str name "-le")) (meta name))]
    `(do
       (defn ~name
         ~m
         (~(with-meta [] {:tag 'long}) ~sizeof)
         ([~buf]
          (if (>= (.remaining ~buf) ~sizeof)
            (let [~ret ~getter]
              [~post-decoder ~buf])
            (reduced ~buf)))
         ([~buf ~(with-meta val {:tag tag})]
          (let [~ret ~pre-encoder] ~setter)))
       (defn ~name-be
         ~m
         (~(with-meta [] {:tag 'long}) ~sizeof)
         ([~buf]
          (if (>= (.remaining ~buf) ~sizeof)
            (let [~buf (.order ~buf ByteOrder/BIG_ENDIAN)
                  ~ret ~getter]
              [~post-decoder ~buf])
            (reduced ~buf)))
         ([~buf ~(with-meta val {:tag tag})]
          (let [~buf (.order ~buf ByteOrder/BIG_ENDIAN)
                ~ret ~pre-encoder]
            ~setter)))
       (defn ~name-le
         ~m
         (~(with-meta [] {:tag 'long}) ~sizeof)
         ([~buf]
          (if (>= (.remaining ~buf) ~sizeof)
            (let [~buf (.order ~buf ByteOrder/LITTLE_ENDIAN)
                  ~ret ~getter]
              [~post-decoder ~buf])
            (reduced ~buf)))
         ([~buf ~(with-meta val {:tag tag})]
          (let [~buf (.order ~buf ByteOrder/LITTLE_ENDIAN)
                ~ret ~pre-encoder]
            ~setter))))))

(def-primitive-codec bool 1 (.get %) (.put %1 %2)
  :pre-encoder (if %2 (byte 1) (byte 0))
  :post-decoder (zero? %2))

(def-primitive-codec ^long i8 1 (.get %) (.put %1 %2)
  :pre-encoder (byte %2)
  :post-decoder (long %2))

(def-primitive-codec ^long i16 2 (.getShort %) (.putShort %1 %2)
  :post-decoder (long %2))

(def-primitive-codec ^long i32 4 (.getInt %) (.putInt %1 %2))

(def-primitive-codec ^long i64 8 (.getLong %) (.putLong %1 %2))

(def-primitive-codec ^long u8 1 (.get %) (.put %1 %2)
  :pre-encoder (unchecked-byte %2)
  :post-decoder (Byte/toUnsignedLong %2))

(def-primitive-codec ^long u16 2 (.getShort %) (.putShort %1 %2)
  :pre-encoder (unchecked-short %2)
  :post-decoder (Short/toUnsignedLong %2))

(def-primitive-codec ^long u32 4 (.getInt %) (.putInt %1 %2)
  :pre-encoder (unchecked-int %2)
  :post-decoder (Integer/toUnsignedLong %2))

(def-primitive-codec ^long u64 8 (.getLong %) (.putLong %1 %2)
  :pre-encoder (unchecked-long %2)
  :post-decoder (Long/divideUnsigned %2 1))

(def-primitive-codec ^double f32 4 (.getFloat %) (.putFloat %1 %2))

(def-primitive-codec ^double f64 8 (.getDouble %) (.putDouble %1 %2))

(def-primitive-codec ^long u24 3
  (bit-or (bit-shift-left (bit-and (.get %) 0xff) 16)
          (bit-shift-left (bit-and (.get %) 0xff) 8)
          (bit-and (.get %) 0xff))
  (doto %1
    (.put (unchecked-byte (unsigned-bit-shift-right %2 16)))
    (.put (unchecked-byte (unsigned-bit-shift-right %2 8)))
    (.put (unchecked-byte %2))))

(def-primitive-codec ^long i24 3 (u24 %) (u24 %1 %2)
  :post-decoder (if (zero? (bit-and (long %2) 0x800000))
                  (bit-or (long %2) 0xff000000)
                  %2))

(defn fast-forward
  [^Buffer buf ^long n]
  (.position buf (+ (.position buf) n)))

(def identity
  (fn
    ([] 0)
    ([^ByteBuffer buf]
     (if (>= (.remaining buf) (.limit buf))
       [(.slice buf) (fast-forward buf (.limit buf))]
       (reduced buf)))
    ([^ByteBuffer buf val] buf)))

(defprotocol ByteRepresentable
  (^long -sizeof [o]))

(extend-protocol ByteRepresentable
  clojure.lang.Sequential
  (-sizeof [coll] (reduce + (map -sizeof coll)))
  
  clojure.lang.PersistentVector
  (-sizeof [coll] (reduce + (map -sizeof coll)))

  clojure.lang.PersistentArrayMap
  (-sizeof [coll] (-sizeof (vals coll)))

  clojure.lang.PersistentHashMap
  (-sizeof [coll] (-sizeof (vals coll)))

  clojure.lang.IFn
  (-sizeof [f] (f))

  String
  (-sizeof [s] (count s))

  Byte
  (-sizeof [n] 1)

  Short
  (-sizeof [n] 2)

  Integer
  (-sizeof [n] 4)

  Long
  (-sizeof [n] 8)

  Float
  (-sizeof [n] 4)

  Double
  (-sizeof [n] 8)

  BigInteger
  (-sizeof [n] (Math/ceil (/ (inc (.bitLength n)) 8))))

(defn sizeof
  (^long [] 0)
  (^long [val] (-sizeof val))
  ([val & more] (transduce (map -sizeof) + (-sizeof val) more)))

(defn decode
  [^Buffer buf codec]
  (first (codec (.rewind buf))))

(defn decode-all
  ([buf codecs] (decode-all buf codecs (reduced buf)))
  ([^Buffer buf codecs not-found]
   (if (.hasRemaining buf)
     (reduce (fn [result codec]
               (let [ret (if (fn? codec)
                           (codec (result 1))
                           [codec (result 1)])]
                 (if (reduced? ret)
                   (cond
                     (.hasRemaining ^Buffer @ret) (assoc result 1 @ret)
                     (.hasRemaining buf) (reduced (assoc result 1 buf))
                     :else (reduced result))
                   (-> result
                       (update 0 conj (ret 0))
                       (assoc 1 (ret 1))))))
             [[] buf] codecs)
     (reduced buf))))

(defn encode
  ([codec val]
   (let [len (max (sizeof codec) (sizeof val))]
     (encode (buffers/byte-buffer len) codec val)))
  ([buf codec val] (codec buf val)))

(defn encode-all
  [buf codecs vals]
  (transduce (partition-all 2)
             (completing
               (fn [result [codec val]]
                 (codec buf val)))
             buf (interleave codecs vals)))

(defn sequential
  [& codecs]
  (let [len (apply sizeof (filter fn? codecs))
        cnt (count codecs)]
    (fn
      (^long [] len)
      ([^Buffer buf]
       (if (.hasRemaining buf)
         (reduce (fn [result codec]
                   (let [ret (if (fn? codec)
                               (codec (result 1))
                               [codec (result 1)])]
                     (if (reduced? ret)
                       (cond
                         (.hasRemaining ^Buffer @ret) (assoc result 1 @ret)
                         (.hasRemaining buf) (reduced (assoc result 1 buf))
                         :else (reduced result))
                       (-> result
                           (update 0 conj (ret 0))
                           (assoc 1 (ret 1))))))
                 [[] buf] codecs)
         [[] buf]))
      ([buf vals] (encode-all buf codecs)))))

(defn ordered-map
  [& keyvals]
  (let [[ks codecs] (transduce (partition-all 2)
                               (completing
                                 (fn [kvs [k v]]
                                   (-> kvs
                                       (update 0 conj k)
                                       (update 1 conj v))))
                               [[] []] keyvals)
        codec (apply sequential codecs)
        len (codec)]
    (fn
      ([] len)
      ([buf] (update (codec buf) 0 #(zipmap ks %)))
      ([buf m] (codec buf (map m ks))))))

(defn finite-block
  [prefix-or-len]
  (let [prefix (when-not (number? prefix-or-len) prefix-or-len)
        len (if prefix 0 prefix-or-len)]
    (if prefix
      (fn
        (^long [] len)
        ([^ByteBuffer buf]
         (let [prefix (prefix buf)]
           (if (>= (.remaining buf) (long (prefix 0)))
             (let [block (doto (.slice buf)
                           (.limit (prefix 0)))]
               [block buf])
             (reduced buf))))
        ([^ByteBuffer to ^ByteBuffer from]
         (prefix to (.capacity from))
         (.put to from)))
      (fn
        (^long [] len)
        ([^ByteBuffer buf]
         (if (>= (.remaining buf) (long prefix-or-len))
           (let [block (doto (.slice buf)
                         (.position (+ (.position buf) (long prefix-or-len))))]
             [block buf])
           (reduced buf)))
        ([^ByteBuffer to ^ByteBuffer from] (.put to from))))))

(defn finite-frame
  [prefix-or-len codec]
  (let [block-codec (finite-block prefix-or-len)
        len (long (if (number? prefix-or-len) prefix-or-len 0))]
    (fn
      (^long [] len)
      ([buf]
       (let [block (block-codec buf)]
         (if (reduced? block)
           block
           (let [ret (codec (block 0))]
             (if (reduced? ret)
               ret
               [(ret 0) (fast-forward buf (.position ^Buffer (ret 1)))])))))
      ([buf val] (codec buf val)))))

(defn prefix
  ([codec] (prefix codec identity identity))
  ([codec to-count from-count]
   (let [codec (if (integer? codec)
                 (finite-block codec)
                 codec)
         len (sizeof codec)]
     (fn
       (^long [] len)
       ([^Buffer buf]
        (if (.hasRemaining buf)
          (update (codec buf) 0 to-count)
          (reduced buf)))
       ([buf n] (codec buf (from-count n)))))))

(defn delimiter-bytes
  [delim]
  (cond
    (buffers/byte-array? delim) (when (pos? (alength ^bytes delim))
                                  (vec delim))
    
    (instance? ByteBuffer delim) (recur (.array ^ByteBuffer delim))

    (string? delim) (-> (.encode (Charset/defaultCharset) ^String delim)
                        (.array)
                        (recur))
    
    (char? delim) (recur (str delim))

    (instance? BigInteger delim) (recur (.toByteArray ^BigInteger delim))
    
    (integer? delim) (recur (biginteger delim))
    
    (float? delim) (recur (.unscaledValue (bigdec delim)))

    (number? delim) (recur (.unscaledValue (bigdec delim)))

    (coll? delim) (some->> delim
                           (into [] (comp (map delimiter-bytes)
                                          (remove nil?)))
                           sort
                           vec)
    
    :else (throw (ex-info "Cannot represent delimiter as Buffer."
                          {:delimiter delim}))))

(defn- prefix-tree
  ([coll]
   (transduce (comp (distinct) (mapcat xf/subvecs) (distinct))
              (completing
                (fn [trie prefix]
                  (if (get-in trie prefix)
                    trie
                    (update-in trie prefix assoc prefix prefix))))
              {} coll)))

(defn- terminal?
  [trie ^long byte]
  (and (== (count trie) 1)
       (let [k (val (first trie))]
         (when (vector? k)
           (== (long (peek k)) byte)))))

(defn- delimiting
  [m ^long n ^long byte]
  (if-let [m (get m byte)]
    (if (terminal? m byte)
      (reduced n)
      m)
    (reduced nil)))

(defn- delimited-block1
  [^ByteBuffer buf trie strip-delimiters?]
  (let [slice (.slice buf)
        end (reduce-kv (fn [^long end ^long n ^long byte]
                         (if-let [m (get trie byte)]
                           (if (terminal? m byte)
                             (if strip-delimiters?
                               (reduced end)
                               (reduced n))
                             (if-let [len (some->> (.position slice (inc n))
                                                   (reduce-kv delimiting m))]
                               (if strip-delimiters?
                                 (reduced (dec n))
                                 (reduced (+ n (long len))))
                               n))
                           n))
                       0 slice)]
    (when (pos? ^long end)
      (.position (.limit slice (inc ^long end)) 0))))

(defn delimited-block
  ([delimiters] (delimited-block delimiters true))
  ([delimiters strip-delimiters?]
   (let [delimiters (delimiter-bytes delimiters)
         longest-length (count (peek delimiters))
         trie (prefix-tree delimiters)]
     (fn
       (^long [] longest-length)
       ([^ByteBuffer buf]
        (if (.hasRemaining buf)
          (if-let [block (delimited-block1 buf trie strip-delimiters?)]
            [block (fast-forward buf (inc (.limit ^Buffer block)))]
            (reduced (fast-forward buf longest-length)))
          (reduced buf)))
       ([^ByteBuffer to ^ByteBuffer from] (.put to from))))))

(defn delimited-frame
  [delimiters codec]
  (let [block-codec (delimited-block delimiters true)]
    (fn
      (^long [] 0)
      ([^ByteBuffer buf]
       (let [block (block-codec buf)]
         (if (reduced? block)
           block
           (codec block))))
      ([^ByteBuffer buf val] (codec buf val)))))

(defn header
  [codec header->body body->header]
  (let [len (sizeof codec)]
    (fn
      (^long [] len)
      ([buf]
       (let [ret (codec buf)]
         (if (reduced? ret)
           ret
           ((header->body (ret 0)) buf))))
      ([buf val] (codec buf (body->header val))))))

(defn repeated
  [codec & {:keys [length prefix delimiters]}]
  (let [len (sizeof codec)]
    (cond
      prefix (fn
               (^long [] (+ len (sizeof prefix)))
               ([buf]
                (let [prefix (prefix buf)]
                  (if (reduced? prefix)
                    prefix
                    (let [n (prefix 0)]
                      (decode-all buf (repeat n codec))))))
               ([buf vals] (encode-all buf (repeat (count vals) codec))))
      length (fn
               (^long [] len)
               ([buf] (decode-all buf (repeat length codec)))
               ([buf vals] (encode-all buf (repeat (count vals) codec) vals)))
      :else (fn
              (^long [] len)
              ([buf] (decode-all buf (repeat codec)))
              ([buf vals]
               (encode-all buf (repeat (count vals) codec) vals))))))

(defn- string-codec
  [^Charset charset ^long len]
  (fn
    (^long [] len)
    ([^ByteBuffer buf]
     (if (zero? len)
       [(.toString (.decode charset buf)) buf]
       (if (>= (.remaining buf) len)
         (let [in (.limit (.slice buf) len)
               out (.decode charset in)]
           [(.toString out) (.position buf (+ (.position buf) len))])
         (reduced buf))))
    ([^ByteBuffer buf ^CharSequence val]
     {:pre [(instance? CharSequence val)]}
     (.put buf (.encode charset (java.nio.CharBuffer/wrap val))))))

(defn- delimited-string-codec
  [^Charset charset ^long len delimeters]
  (let [len (long len)
        block-codec (delimited-block delimeters)
        str-codec (string-codec charset len)]
    (fn
      (^long [] len)
      ([buf]
       (let [block (block-codec buf)]
         (if (reduced? block)
           block
           [((str-codec (block 0)) 0) buf])))
      ([buf val] (str-codec buf val)))))

(defn string
  [charset & {:keys [length delimiters prefix suffix]}]
  (let [^java.nio.charset.Charset charset (if (instance? Charset charset)
                                            charset
                                            (Charset/forName (name charset)))
        sizeof (if length
                 (* (long (.maxBytesPerChar (.newEncoder charset)))
                    (long length))
                 0)]
    (if (seq delimiters)
      (delimited-string-codec charset sizeof delimiters)
      (string-codec charset sizeof))))
