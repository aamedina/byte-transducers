(ns byte-transducers.generic
  (:refer-clojure :exclude [identity]))

(defn identity
  [rf]
  (fn
    ([] (rf))
    ([result] (rf result))
    ([result input] (rf result input))))

(defn unfold
  ([f]
   (fn [rf]
     (fn
       ([] (rf))
       ([result] (rf result))
       ([result seed]
        (let [input (f seed)]
          (if (reduced? input)
            (rf result (unreduced input))
            (recur (rf result input) input)))))))
  ([f pred]
   (let [f (fn [input]
             (if (pred input)
               (f input)
               (reduced input)))]
     (unfold f))))

(defn transducing
  ([xf reducible]
   (reify clojure.lang.IReduce
     (reduce [this f]
       (.reduce this f (f)))
     (reduce [this f init]
       (transduce xf (completing f) init reducible))))
  ([xf xg & more]
   (if (seq more)
     (recur (comp xf xg) (first more) (next more))
     (transducing xf xg))))

(defn preserving-reduced
  [rf]
  (fn [result input]
    (let [ret (rf result input)]
      (if (reduced? ret)
        (reduced ret)
        ret))))

(defn subvecs
  [v]
  (let [len (count v)]
    (conj (into [] (map (fn [^long n] (subvec v 0 n))) (range 1 len)) v)))

(defn rsubvecs
  [v]
  (let [len (count v)]
    (into [v] (map (fn [^long n] (subvec v 0 n))) (range (dec len) 0 -1))))
