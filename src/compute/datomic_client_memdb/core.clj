(ns compute.datomic-client-memdb.core
  (:require
    [datomic.client.api :as client]
    [datomic.client.api.protocols :as client-proto]
    [datomic.client.api.impl :as client-impl]
    [datomic.query.support :as q-support]
    [datomic.api :as peer])
  (:import (java.io Closeable)))

(defn- update-vals [m ks f]
  (reduce #(update-in % [%2] f) m ks))

(defn- throw-unsupported
  [data]
  (throw (ex-info "Unsupported operation."
                  (merge {:cognitect.anomalies/category :cognitect.anomalies/unsupported}
                         data))))

(defn collection-query? [query]
  (let [[{fnd :find}] (q-support/parse-as query)]
    (or (and (= 2 (count fnd))
             (= '. (second fnd)))
        (and (vector? (first fnd))
             (= 2 (count (first fnd)))
             (= '... (second (first fnd)))))))

(deftype LocalDb [db]
  client-proto/Db
  (as-of [_ time-point]
    (LocalDb. (peer/as-of db time-point)))
  (datoms [_ arg-map]
    (apply peer/datoms db (:index arg-map) (:components arg-map)))
  (db-stats [_]
    (throw-unsupported {}))
  (history [_]
    (LocalDb. (peer/history db)))
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
    (LocalDb. (peer/since db t)))
  (with [_ arg-map]
    (-> (peer/with db (:tx-data arg-map))
        (update :db-before #(LocalDb. %))
        (update :db-after #(LocalDb. %))))

  client-impl/Queryable
  (q [_ arg-map]
    (let [{:keys [query args]} arg-map]
      (when (collection-query? query)
        (throw (ex-info "Only find-rel elements are allowed in client find-spec, see http://docs.datomic.com/query.html#grammar"
                        {:cognitect.anomalies/category :cognitect.anomalies/incorrect,
                         :cognitect.anomalies/message  "Only find-rel elements are allowed in client find-spec, see http://docs.datomic.com/query.html#grammar"
                         :dbs (filterv #(satisfies? client-proto/Db %) args)})))
      (apply peer/q query (map (fn [x]
                                 (if (instance? LocalDb x)
                                   (.-db x)
                                   x))
                               args))))

  clojure.lang.ILookup
  (valAt [this k]
    (.valAt this k nil))
  (valAt [this k not-found]
    (case k
      :t (peer/basis-t db)
      :next-t (inc (:t this))
      not-found)))


(deftype LocalConnection [conn]
  client-proto/Connection
  (db [_]
    (LocalDb. (peer/db conn)))

  (transact [_ arg-map]
    (-> @(peer/transact conn (:tx-data arg-map))
        (update-vals #{:db-before :db-after} #(LocalDb. %))))

  (tx-range [_ arg-map]
    (peer/tx-range (peer/log conn) (:start arg-map) (:end arg-map)))

  (with-db [this]
    (client/db this))

  clojure.lang.ILookup
  (valAt [this k]
    (.valAt this k nil))
  (valAt [this k not-found]
    (get (client/db this) k not-found)))



(defrecord Client [uri]
  client-proto/Client
  (administer-system [_ arg-map] (throw-unsupported {}))
  (list-databases [_ _]
    (or (peer/get-database-names uri) (list)))

  (connect [_client arg-map]
    (LocalConnection. (peer/connect uri)))

  (create-database [_ _arg-map]
    (peer/create-database uri)
    true)

  (delete-database [_ _arg-map]
    (peer/delete-database uri)
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
  [params]
  (map->Client params))
