(ns compute.datomic-client-memdb.core
  (:require
    [datomic.client.api :as client]
    [datomic.client.api.protocols :as client-proto]
    [datomic.client.api.impl :as client-impl]
    [datomic.api :as peer])
  (:import (java.io Closeable)))

(defn- update-vals [m ks f]
  (reduce #(update-in % [%2] f) m ks))

(defn- throw-unsupported
  [data]
  (throw (ex-info "Unsupported operation."
                  (merge {:cognitect.anomalies/category :cognitect.anomalies/unsupported}
                         data))))

(defn memdb-uri
  "Returns a Datomic mem database URI for `db-name`."
  [db-name]
  (str "datomic:mem://" db-name))

(defn free-uri
  "Returns the default Datomic free database URI for `db-name`."
  [db-name]
  (str "datomic:free://localhost:4334/" db-name))

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
    ;; Datomic free will return nil when pull'ing for a single attribute that does
    ;; not exist on an entity. Datomic Cloud returns {} in this case. Since there
    ;; isn't any cases known where Datomic Cloud will return nil for a pull, we
    ;; simply return an empty map if nil is returned from the Peer API.
    (if eid
      (or (peer/pull db selector eid) {})
      (throw (ex-info "Expected value for :eid"
                      {:cognitect.anomalies/category                      :cognitect.anomalies/incorrect
                       :cognitect.anomalies/message                       "Expected value for :eid"
                       :datomic.client.impl.shared.validator/got          {:selector selector :eid eid}
                       :datomic.client.impl.shared.validator/op           :pull
                       :datomic.client.impl.shared.validator/requirements '{:eid value :selector value}}))))
  (since [_ t]
    (LocalDb. (peer/since db t) db-name))
  (with [_ arg-map]
    (-> (peer/with db (:tx-data arg-map))
        (update :db-before #(LocalDb. % db-name))
        (update :db-after #(LocalDb. % db-name))))

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


(defrecord Client [db-name-as-uri-fn]
  client-proto/Client
  (administer-system [_ arg-map] (throw-unsupported {}))
  (list-databases [_ _]
    (or (peer/get-database-names (db-name-as-uri-fn "*")) (list)))

  (connect [client arg-map]
    (let [db-name (:db-name arg-map)]
      (if (contains? (set (client/list-databases client {})) db-name)
        (LocalConnection. (peer/connect (db-name-as-uri-fn db-name)) db-name)
        (let [msg (format "Unable to find keyfile %s. Make sure that your endpoint and db-name are correct." db-name)]
          (throw (ex-info msg {:cognitect.anomalies/category :cognitect.anomalies/not-found
                               :cognitect.anomalies/message  msg}))))))

  (create-database [_ arg-map]
    (let [db-name (:db-name arg-map)]
      (peer/create-database (db-name-as-uri-fn db-name)))
    true)

  (delete-database [_ arg-map]
    (peer/delete-database (db-name-as-uri-fn (:db-name arg-map)))
    true)

  Closeable
  (close [client]
    (doseq [db (client/list-databases client {})]
      (client/delete-database client {:db-name db}))
    nil))

(defn close
  "Cleans up the Datomic Peer Client by deleting all databases."
  [client]
  (.close client))

(defn client
  "Returns a Client record that implements the Datomic Client API Protocol. Optionally
  takes :db-name-as-uri-fn which is a function that is passed a db-name and is
  expected to return a Datomic Peer database URI. Note that this function is passed
  a '*' when list-databases is called."
  [arg-map]
  (map->Client {:db-name-as-uri-fn (or (:db-name-as-uri-fn arg-map) memdb-uri)}))
