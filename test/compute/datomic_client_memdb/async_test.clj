(ns compute.datomic-client-memdb.async-test
  (:require
    [clojure.test :refer :all]
    [clojure.core.async :as async]
    [datomic.client.api.async :as d.async]
    [datomic.client.api.protocols :as client-proto]
    [datomic.client.impl.shared.protocols :as async-proto]
    [compute.datomic-client-memdb.core :as memdb]
    [compute.datomic-client-memdb.async :as memdb.async])
  (:import (clojure.lang ExceptionInfo)))

(def ^:dynamic *client* nil)

(defn client-fixture
  [f]
  (with-open [c (memdb.async/client {})]
    (binding [*client* c]
      (f))))

(defn anom->ex
  [anom]
  (let [cat (:cognitect.anomalies/category anom)
        default-msg (str "A " cat " anomaly occurred.")
        ex-msg (or (:cognitect.anomalies/message anom) default-msg)]
    (if-let [ex (:ex anom)]
      (ex-info ex-msg anom ex)
      (ex-info ex-msg anom))))

(defn <t!!
  ([ch] (<t!! ch 100))
  ([ch timeout-ms]
   (let [[v port] (async/alts!! [ch (async/timeout timeout-ms)])]
     (if (= port ch)
       (if (:cognitect.anomalies/category v)
         (throw (anom->ex v))
         v)
       ::timeout))))

(use-fixtures :each client-fixture)

(deftest client-test
  (is (= true (<t!! (d.async/create-database *client* {:db-name "test"}))))
  (is (= (list "test") (<t!! (d.async/list-databases *client* {}))))
  (is (satisfies? async-proto/Connection (<t!! (d.async/connect *client* {:db-name "test"}))))
  (is (= true (<t!! (d.async/delete-database *client* {:db-name "test"}))))
  (is (= (list) (<t!! (d.async/list-databases *client* {}))))
  (is (thrown? ExceptionInfo (<t!! (d.async/connect *client* {:db-name "test"}))))
  (let [client2 (memdb.async/client {:db-name-as-uri-fn
                                     (fn [db-name]
                                       (str (memdb/memdb-uri db-name)
                                            ;; a * is passed as the db-name when list-databases is used
                                            (when (not= db-name "*") "1")))})]
    (is (= true (<t!! (d.async/create-database client2 {:db-name "foo"}))))
    (is (= (list "foo1") (<t!! (d.async/list-databases client2 {})))
        "ensure that our custom db-name-as-uri-fn function is called")))

(def test-schema [{:db/ident       :user/name
                   :db/valueType   :db.type/string
                   :db/cardinality :db.cardinality/one
                   :db/index       true}])

(deftest connection-test
  (<t!! (d.async/create-database *client* {:db-name "test"}))
  (let [conn (<t!! (d.async/connect *client* {:db-name "test"}))]
    (testing "db is a db"
      (is (satisfies? async-proto/Db (d.async/db conn))))
    (testing "transaction returns tx-report"
      (is (= #{:db-before :db-after :tx-data :tempids}
             (set (keys (<t!! (d.async/transact conn {:tx-data test-schema})))))))
    (testing "tx-range elements contain :t and :data"
      (is (= #{:t :data}
             (-> conn (d.async/tx-range {}) (<t!!) (first) (keys) (set)))))
    (testing "with-db is a db"
      (is (satisfies? client-proto/Db (<t!! (d.async/with-db conn)))))
    (testing "conn info works"
      (is (every? some? (map #(get conn %) [:t :next-t :db-name]))))))

(deftest db-test
  (<t!! (d.async/create-database *client* {:db-name "test"}))
  (let [conn (<t!! (d.async/connect *client* {:db-name "test"}))
        _ (<t!! (d.async/transact conn {:tx-data test-schema}))
        tx-report (<t!! (d.async/transact conn {:tx-data [{:db/id     "bob"
                                                           :user/name "bob"}]}))
        bob-id (get-in tx-report [:tempids "bob"])
        db (d.async/db conn)]
    (testing "query works"
      (is (= #{[bob-id]}
             (<t!!
               (d.async/q
                 {:query '[:find ?e
                           :where
                           [?e :user/name "bob"]]
                  :args  [db]})))))
    (testing "pull works"
      (is (= {:user/name "bob"}
             (<t!! (d.async/pull db {:selector '[:user/name]
                                     :eid      bob-id}))))
      (is (= {}
             (<t!! (d.async/pull db {:selector '[:user/i-dont-exist]
                                     :eid      bob-id})))
          "nonexistent attribute results in empty map, not nil")
      (is (thrown? ExceptionInfo (<t!! (d.async/pull db {:selector [:user/name]
                                                         :eid      nil})))
          "exception thrown when pulling nil eid"))
    (testing "db info works"
      (is (every? some? (map #(get db %) [:t :next-t :db-name])))
      (is (:t (last (<t!! (d.async/tx-range conn {}))))
          (:t conn)))
    (testing "db with works"
      (let [tx-report (<t!! (d.async/with db {:tx-data [{:user/name "bob5"}]}))]
        (is (:db-before tx-report))
        (is (:db-after tx-report))
        (is (:tempids tx-report))
        (is (:tx-data tx-report))))
    (testing "index-range works"
      (let [[_ _ value] (first (<t!! (d.async/index-range db {:attrid [:db/ident :user/name]})))]
        (is (= value "bob"))))))

