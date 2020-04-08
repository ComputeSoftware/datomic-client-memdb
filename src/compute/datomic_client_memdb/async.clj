(ns compute.datomic-client-memdb.async
  (:require
    [clojure.core.async :as async]
    [datomic.client.api :as d]
    [datomic.client.impl.shared.protocols :as async-proto]
    [compute.datomic-client-memdb.core :as memdb])
  (:import (java.io Closeable)
           (clojure.lang ExceptionInfo)))

(defn- wrap-async
  [f]
  (async/thread
    (try
      (f)
      (catch Throwable ex
        (if (and (instance? ExceptionInfo ex)
                 (:cognitect.anomalies/category (ex-data ex)))
          (ex-data ex)
          {:cognitect.anomalies/category :cognitect.anomalies/fault
           :cognitect.anomalies/message  (.getMessage ex)
           :ex                           ex})))))

(deftype LocalDb [conn db]
  async-proto/Db
  (as-of [_ time-point]
    (d/as-of db time-point))
  (datoms [_ arg-map]
    (wrap-async #(d/datoms db arg-map)))
  (db-stats [_]
    (doto (async/promise-chan)
      (async/put! {:cognitect.anomalies/category :cognitect.anomalies/unsupported
                   :cognitect.anomalies/message  "db-stats is not implemented for datomic-client-memdb"})))
  (history [_]
    (d/history db))
  (index-range [_ arg-map]
    (wrap-async #(d/index-range db arg-map)))
  (pull [_ arg-map]
    (wrap-async #(d/pull db arg-map)))
  (since [_ t]
    (d/since db t))
  (with [_ arg-map]
    (wrap-async #(d/with db arg-map)))

  async-proto/ParentConnection
  (-conn [_] conn)

  clojure.lang.ILookup
  (valAt [_ k]
    (.valAt db k))
  (valAt [_ k not-found]
    (.valAt db k not-found)))

(deftype LocalConnection [conn]
  async-proto/Connection
  (db [this]
    (wrap-async #(LocalDb. this (d/db conn))))
  (log [_]
    (doto (async/promise-chan)
      (async/put! {:cognitect.anomalies/category :cognitect.anomalies/unsupported
                   :cognitect.anomalies/message  "log is not implemented for datomic-client-memdb"})))
  (q [_ arg-map]
    (wrap-async
      #(d/q (update arg-map :args (fn [args]
                                    (map (fn [x]
                                           (if (async-proto/-conn x)
                                             (.-db x)
                                             x))
                                         args))))))
  (tx-range [_ arg-map]
    (wrap-async #(d/tx-range conn arg-map)))
  (transact [_ arg-map]
    (wrap-async #(d/transact conn arg-map)))
  (recent-db [this]
    (async-proto/db this))
  (sync [_ t]
    (doto (async/promise-chan)
      (async/put! {:cognitect.anomalies/category :cognitect.anomalies/unsupported
                   :cognitect.anomalies/message  "recent-db is not implemented for datomic-client-memdb"})))
  (with-db [_]
    (wrap-async #(d/with-db conn)))

  clojure.lang.ILookup
  (valAt [_ k]
    (.valAt conn k))
  (valAt [_ k not-found]
    (.valAt conn k not-found)))

(defrecord Client [client]
  async-proto/Client
  (list-databases [_ argm]
    (wrap-async #(d/list-databases client argm)))
  (connect [_ arg-map]
    (wrap-async #(LocalConnection. (d/connect client arg-map))))
  (create-database [_ arg-map]
    (wrap-async #(d/create-database client arg-map)))
  (delete-database [_ arg-map]
    (wrap-async #(d/delete-database client arg-map)))
  Closeable
  (close [_]
    (.close client)))

(defn client
  "Returns a Client record that implements the Datomic Client API Protocol. Optionally
  takes :db-name-as-uri-fn which is a function that is passed a db-name and is
  expected to return a Datomic Peer database URI. Note that this function is passed
  a '*' when list-databases is called."
  [arg-map]
  (map->Client {:client (memdb/client arg-map)}))