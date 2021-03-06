(ns byte-transducers.codecs-test
  (:refer-clojure :exclude [identity chunk])
  (:require [clojure.test :refer :all]
            [byte-transducers.codecs :refer :all]
            [byte-transducers.io :as io]))

(set! *warn-on-reflection* true)

(set! *unchecked-math* :warn-on-boxed)

(def c-string (string :ascii :delimiters ["\0"]))
(def vec2 (sequential f32-le f32-le))
(def vec3 (sequential f32-le f32-le f32-le))
(def vec4 (sequential f32-le f32-le f32-le f32-le))
(def uv (sequential i32-le i32-le))
(def uvw (sequential i8-le i8-le i8-le))
(def quaternion (sequential f32-le f32-le f32-le f32-le))
(def color (sequential u8-le u8-le u8-le u8-le))
(def tag (string :ascii :length 4))

(defn chunk
  [type-id codec]
  (let [codec (finite-frame u32-be codec)]
    (header tag
      (fn [header]
        (if (= header type-id)
          (ordered-map
            :tag header
            :content codec)
          (fn
            ([] 0)
            ([^java.nio.ByteBuffer buf]
             (reduced (.position buf (- (.position buf) 4))))
            ([buf val] buf))))
      (constantly type-id))))

(defmulti form-codec clojure.core/identity)

(defmethod form-codec :default
  [_]
  (fn
    ([] 0)
    ([buf] [buf buf])
    ([buf val] buf)))

(defn form
  ([] (form nil nil))
  ([codec] (form nil codec))
  ([type-id codec]
   (chunk "FORM" (header tag
                   (fn [header]
                     (ordered-map
                       :tag header
                       :content (or codec (form-codec header))))
                   :tag))))

(def skmg-info
  (ordered-map
    :max-transforms-per-vertex i32-le
    :max-transforms-per-shader i32-le
    :skeleton-template-names i32-le
    :transform-names i32-le
    :positions i32-le
    :transform-weight-data i32-le
    :normals i32-le
    :per-shader-data i32-le
    :blend-targets i32-le
    :occlusion-zone-names i16-le
    :occlusion-zone-combinations i16-le
    :zones-this-occludes i16-le
    :occlusion-layer i16-le))

(def triangles
  (repeated i32 :prefix (prefix i32-le (partial * 3) (partial / 3))))

(def prim
  (sequential
    (chunk "INFO" i32-le)
    (chunk "ITL " triangles)))

(def psdt
  (sequential
    (chunk "NAME" c-string)
    (chunk "PIDX" (repeated i32-le :prefix i32-le))
    (chunk "NIDX" (repeated i32-le))
    (chunk "DOT3" (repeated i32-le))
    (chunk "VDCL" (repeated color))
    (chunk "TXCI" (repeated i32-le :prefix i32-le))
    (form "TCSF" (chunk "TCSD" (repeated f32-le)))
    (form "PRIM" prim)))

(def skmg
  (form "0004"
    (sequential
      (chunk "INFO" skmg-info)
      (chunk "SKTM" (repeated c-string))
      (chunk "XFNM" (repeated c-string))
      (chunk "POSN" (repeated vec3))
      (chunk "TWHD" (repeated i32-le))
      (chunk "TWDT" (repeated (sequential i32-le f32-le)))
      (chunk "NORM" (repeated vec3))
      (chunk "DOT3" (repeated vec4 :prefix i32-le))
      (repeated (form "PSDT" psdt)))))

(defmethod form-codec "SKMG"
  [_]
  skmg)
