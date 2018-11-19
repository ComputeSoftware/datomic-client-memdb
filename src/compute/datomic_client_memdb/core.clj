(ns compute.datomic-client-memdb.core
  (:require
    [datomic.client.api :as client]
    [datomic.client.api.protocols :as client-proto]
    [datomic.client.api.impl :as client-impl]
    [datomic.api :as peer])
  (:import (java.io Closeable)))

(defonce clients (atom {}))

(defn update-vals [m ks f]
  (reduce #(update-in % [%2] f) m ks))

(defn- throw-unsupported
  [data]
  (throw (ex-info "Unsupported operation." data)))

(defn- memdb-uri
  [db-name]
  (str "datomic:mem://" db-name))

(deftype LocalDb [db db-name]
  client-proto/Db
  (as-of [_ time-point]
    (LocalDb. (peer/as-of db time-point) db-name))
  (datoms [_ arg-map]
    (apply peer/datoms db (:index arg-map) (:components arg-map)))
  (db-stats [_]
    (throw-unsupported {}))
  (history [_]
    (LocalDb. (peer/history db) db-name))
  (index-range [_ arg-map]
    (peer/index-range db (:attrid arg-map) (:start arg-map) (:end arg-map)))
  (pull [this arg-map]
    (client/pull this (:selector arg-map) (:eid arg-map)))
  (pull [_ selector eid]
    (peer/pull db selector eid))
  (since [_ t]
    (LocalDb. (peer/since db t) db-name))
  (with [_ arg-map]
    (LocalDb. (peer/with db (:tx-data arg-map)) db-name))

  client-impl/Queryable
  (q [_ arg-map]
    (apply peer/q (:query arg-map) (map (fn [x]
                                          (if (instance? LocalDb x)
                                            (.-db x)
                                            x))
                                        (:args arg-map))))

  clojure.lang.ILookup
  (valAt [this k]
    (.valAt this k nil))
  (valAt [this k not-found]
    (case k
      :t (peer/basis-t db)
      :next-t (inc (:t this))
      :db-name db-name
      not-found)))


(deftype LocalConnection [conn db-name]
  client-proto/Connection
  (db [_]
    (LocalDb. (peer/db conn) db-name))

  (transact [_ arg-map]
    (-> @(peer/transact conn (:tx-data arg-map))
        (update-vals #{:db-before :db-after} #(LocalDb. % db-name))))

  (tx-range [_ arg-map]
    (peer/tx-range (peer/log conn) (:start arg-map) (:end arg-map)))

  (with-db [this]
    (client/db this))

  clojure.lang.ILookup
  (valAt [this k]
    (.valAt this k nil))
  (valAt [this k not-found]
    (get (client/db this) k not-found)))


(defrecord Client [db-lookup client-arg-map]
  client-proto/Client
  (list-databases [_ _]
    (or (keys @db-lookup) '()))

  (connect [_ arg-map]
    (if-let [db-uri (get @db-lookup (:db-name arg-map))]
      (LocalConnection. (peer/connect db-uri) (:db-name arg-map))
      (throw (ex-info "Unable to find db." {:db-name (:db-name arg-map)}))))

  (create-database [_ arg-map]
    (let [db-name (:db-name arg-map)]
      (when-not (get @db-lookup db-name)
        (let [db-uri (memdb-uri (java.util.UUID/randomUUID))]
          (peer/create-database db-uri)
          (swap! db-lookup assoc db-name db-uri))))
    true)

  (delete-database [_ arg-map]
    (swap! db-lookup dissoc (:db-name arg-map))
    true)

  Closeable
  (close [client]
    (doseq [db (client/list-databases client {})]
      (client/delete-database client {:db-name db}))
    (swap! clients dissoc client-arg-map)
    nil))


(defn close
  [client]
  (.close client))

(defn client
  [arg-map]
  (if-let [c (get @clients arg-map)]
    c
    (let [new-client (map->Client {:db-lookup      (atom {})
                                   :client-arg-map arg-map})]
      (swap! clients assoc arg-map new-client)
      new-client)))
