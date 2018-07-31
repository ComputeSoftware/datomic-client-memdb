(ns compute.datomic-client-memdb.core
  (:require
    [datomic.client.api :as client]
    [datomic.client.api.impl :as client-impl]
    [datomic.api :as peer])
  (:import (datomic.peer LocalConnection)
           (datomic.db Db)
           (java.io Closeable)))

(defn- memdb-uri
  [db-name]
  (str "datomic:mem://" db-name))

(defrecord Client [db-lookup]
  client/Client
  (list-databases [_ _]
    (or (keys @db-lookup) '()))

  (connect [_ arg-map]
    (if-let [db-uri (get @db-lookup (:db-name arg-map))]
      (peer/connect db-uri)
      (throw (ex-info "Unable to find db." {:db-name (:db-name arg-map)}))))

  (create-database [_ arg-map]
    (let [db-name (:db-name arg-map)
          db-uri (memdb-uri (java.util.UUID/randomUUID))]
      (peer/create-database db-uri)
      (swap! db-lookup assoc db-name db-uri))
    true)

  (delete-database [_ arg-map]
    (swap! db-lookup dissoc (:db-name arg-map))
    true)

  Closeable
  (close [client]
    (doseq [db (client/list-databases client {})]
      (client/delete-database client {:db-name db}))))

(extend-type LocalConnection
  client/Connection
  (db [conn]
    (peer/db conn))

  (transact [conn arg-map]
    @(peer/transact conn (:tx-data arg-map)))

  (tx-range [conn arg-map]
    (peer/tx-range (peer/log conn) (:start arg-map) (:end arg-map)))

  (with-db [conn]
    (peer/db conn)))

(extend-type Db
  client/Db
  (as-of [db time-point]
    (peer/as-of db time-point))
  (datoms [db arg-map]
    (apply peer/datoms db (:index arg-map) (:components arg-map)))
  (db-stats [db]
    (throw (ex-info "Unsupported operation." {})))
  (history [db]
    (peer/history db))
  (index-range [db arg-map]
    (peer/index-range db (:attrid arg-map) (:start arg-map) (:end arg-map)))
  (pull
    ([db arg-map]
      (client/pull db (:selector arg-map) (:eid arg-map)))
    ([db selector eid]
      (peer/pull db selector eid)))
  (since [db t]
    (peer/since db t))
  (with [db arg-map]
    (peer/with db (:tx-data arg-map)))

  client-impl/Queryable
  (q [db arg-map]
    (apply peer/q (:query arg-map) db (:args arg-map))))

(defn close
  [client]
  (.close client))

(defn client
  [arg-map]
  (map->Client {:db-lookup (atom {})}))