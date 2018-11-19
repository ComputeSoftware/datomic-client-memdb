(ns compute.datomic-client-memdb.core-test
  (:require
    [clojure.test :refer :all]
    [datomic.client.api :as d]
    [datomic.client.api.protocols :as client-proto]
    [compute.datomic-client-memdb.core :as memdb])
  (:import (clojure.lang ExceptionInfo)))

(def ^:dynamic *client* nil)

(defn client-fixture
  [f]
  (with-open [c (memdb/client {})]
    (binding [*client* c]
      (f))))

(use-fixtures :each client-fixture)

(deftest client-test
  (is (d/create-database *client* {:db-name "test"}))
  (testing "calling create-database twice does not create a new db"
    (let [db-lookup @(:db-lookup *client*)]
      (d/create-database *client* {:db-name "test"})
      (is (= db-lookup @(:db-lookup *client*)))))
  (is (= (list "test") (d/list-databases *client* {})))
  (is (satisfies? client-proto/Connection (d/connect *client* {:db-name "test"})))
  (is (d/delete-database *client* {:db-name "test"}))
  (is (= (list) (d/list-databases *client* {})))
  (is (thrown? ExceptionInfo (d/connect *client* {:db-name "test"}))))

(def test-schema [{:db/ident       :user/name
                   :db/valueType   :db.type/string
                   :db/cardinality :db.cardinality/one
                   :db/index       true}])

(deftest connection-test
  (d/create-database *client* {:db-name "test"})
  (let [conn (d/connect *client* {:db-name "test"})]
    (testing "db is a db"
      (is (satisfies? client-proto/Db (d/db conn))))
    (testing "transaction returns tx-report"
      (is (= #{:db-before :db-after :tx-data :tempids}
             (set (keys (d/transact conn {:tx-data test-schema}))))))
    (testing "tx-range elements contain :t and :data"
      (is (= #{:t :data}
             (-> conn (d/tx-range {}) (first) (keys) (set)))))
    (testing "with-db is a db"
      (is (satisfies? client-proto/Db (d/with-db conn))))
    (testing "conn info works"
      (is (every? some? (map #(get conn %) [:t :next-t :db-name]))))))

(deftest db-test
  (d/create-database *client* {:db-name "test"})
  (let [conn (d/connect *client* {:db-name "test"})
        _ (d/transact conn {:tx-data test-schema})
        tx-report (d/transact conn {:tx-data [{:db/id     "bob"
                                               :user/name "bob"}]})
        bob-id (get-in tx-report [:tempids "bob"])
        db (d/db conn)]
    (testing "query works"
      (is (= #{[bob-id]}
             (d/q '[:find ?e
                    :where
                    [?e :user/name "bob"]] db))))
    (testing "db info works"
      (is (every? some? (map #(get db %) [:t :next-t :db-name])))
      (is (:t (last (d/tx-range conn {})))
          (:t conn)))
    (testing "index-range works"
      (let [[_ _ value] (first (d/index-range db {:attrid [:db/ident :user/name]}))]
        (is (= value "bob"))))))

