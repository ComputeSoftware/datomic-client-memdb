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
  (is (= (list "test") (d/list-databases *client* {})))
  (is (satisfies? client-proto/Connection (d/connect *client* {:db-name "test"})))
  (is (d/delete-database *client* {:db-name "test"}))
  (is (= (list) (d/list-databases *client* {})))
  (is (thrown? ExceptionInfo (d/connect *client* {:db-name "test"})))
  (let [client2 (memdb/client {:db-name-as-uri-fn (fn [db-name]
                                                    (str (memdb/memdb-uri db-name)
                                                         ;; a * is passed as the db-name when list-databases is used
                                                         (when (not= db-name "*") "1")))})]
    (is (d/create-database client2 {:db-name "foo"}))
    (is (= (list "foo1") (d/list-databases client2 {}))
        "ensure that our custom db-name-as-uri-fn function is called")))

(def test-schema [{:db/ident       :user/name
                   :db/valueType   :db.type/string
                   :db/cardinality :db.cardinality/one
                   :db/index       true}
                  {:db/ident       :user/heads
                   :db/valueType   :db.type/long
                   :db/cardinality :db.cardinality/one}])

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
                                               :user/name "bob"
                                               :user/heads 1}
                                              {:db/id "alice"
                                               :user/name "alice"
                                               :user/heads 1}]})
        bob-id (get-in tx-report [:tempids "bob"])
        db (d/db conn)]
    (testing "query works"
      (is (= #{[bob-id]}
             (d/q '[:find ?e
                    :where
                    [?e :user/name "bob"]] db))))
    (testing "pull spec plus without :with works"
      (is (= [[#:user{:name "bob"} 1]
              [#:user{:name "alice"} 1]]
             (d/q '[:find (pull ?e [:user/name]) (sum ?heads)
                    :where
                    [?e :user/heads ?heads]]
                  db
                  "bob"))))
    (testing ":with without pull spec works"
      (is (= [[2]]
             (d/q '[:find (sum ?heads)
                    :with ?name
                    :where
                    [?e :user/name ?name]
                    [?e :user/heads ?heads]]
                  db
                  "bob"))))
    (testing "pull spec plus :with is currently broken"
      (is (= [[#:user{:name "bob"} 1]
              [#:user{:name "alice"} 1]]
             (d/q '[:find (pull ?e [:user/name]) (sum ?heads)
                    :with ?name
                    :where
                    [?e :user/name ?name]
                    [?e :user/heads ?heads]]
                  db
                  "bob"))))
    (testing "db info works"
      (is (every? some? (map #(get db %) [:t :next-t :db-name])))
      (is (:t (last (d/tx-range conn {})))
          (:t conn)))
    (testing "db with works"
      (let [tx-report (d/with db {:tx-data [{:user/name "bob5"}]})]
        (is (:db-before tx-report))
        (is (:db-after tx-report))
        (is (:tempids tx-report))
        (is (:tx-data tx-report))))
    (testing "index-range works"
      (let [[_ _ value] (first (d/index-range db {:attrid [:db/ident :user/name]}))]
        (is (= value "alice"))))))
