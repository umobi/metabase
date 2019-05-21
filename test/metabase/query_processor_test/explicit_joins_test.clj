(ns metabase.query-processor-test.explicit-joins-test
  (:require [expectations :refer [expect]]
            [metabase.query-processor-test :as qp.test]
            [metabase.test.data :as data]
            [metabase.test.data.datasets :as datasets]
            [metabase.query-processor :as qp]
            [metabase.driver :as driver]))

(defn- native-form [query]
  (:query (qp/query->native query)))

;; Can we specify an *explicit* JOIN using the default options?
(expect
  (str "SELECT \"PUBLIC\".\"VENUES\".\"ID\" AS \"ID\","
       " \"PUBLIC\".\"VENUES\".\"NAME\" AS \"NAME\","
       " \"PUBLIC\".\"VENUES\".\"CATEGORY_ID\" AS \"CATEGORY_ID\","
       " \"PUBLIC\".\"VENUES\".\"LATITUDE\" AS \"LATITUDE\","
       " \"PUBLIC\".\"VENUES\".\"LONGITUDE\" AS \"LONGITUDE\","
       " \"PUBLIC\".\"VENUES\".\"PRICE\" AS \"PRICE\" "
       "FROM \"PUBLIC\".\"VENUES\" "
       "LEFT JOIN \"PUBLIC\".\"CATEGORIES\" \"CATEGORIES\""
       " ON \"PUBLIC\".\"VENUES\".\"CATEGORY_ID\" = 1 "
       "LIMIT 1048576")
  (native-form
   (data/mbql-query venues
     {:joins [{:source-table $$categories
               :condition    [:= [:field-id $category_id] 1]}]})))

(defn- query-with-strategy [strategy]
  (data/dataset bird-flocks
    (data/mbql-query bird
      {:fields   [[:field-id $name] [:joined-field "f" [:field-id $flock.name]]]
       :joins    [{:source-table $$flock
                   :condition    [:= [:field-id $flock_id] [:joined-field "f" [:field-id $flock.id]]]
                   :strategy     strategy
                   :alias        "f"}]
       :order-by [[:asc [:field-id $name]]]})))

;; Can we supply a custom alias? Can we do a left outer join ??
(datasets/expect-with-drivers (qp.test/non-timeseries-drivers-with-feature :left-join)
  [["Big Red"          "Bayview Brood"]
   ["Callie Crow"      "Mission Street Murder"]
   ["Camellia Crow"    nil]
   ["Carson Crow"      "Mission Street Murder"]
   ["Chicken Little"   "Bayview Brood"]
   ["Geoff Goose"      nil]
   ["Gerald Goose"     "Green Street Gaggle"]
   ["Greg Goose"       "Green Street Gaggle"]
   ["McNugget"         "Bayview Brood"]
   ["Olita Owl"        nil]
   ["Oliver Owl"       "Portrero Hill Parliament"]
   ["Orville Owl"      "Portrero Hill Parliament"]
   ["Oswald Owl"       nil]
   ["Pamela Pelican"   nil]
   ["Patricia Pelican" nil]
   ["Paul Pelican"     "SoMa Squadron"]
   ["Peter Pelican"    "SoMa Squadron"]
   ["Russell Crow"     "Mission Street Murder"]]
  (qp.test/rows
    (qp/process-query
      (query-with-strategy :left-join))))

;; Can we do a right outer join?
(datasets/expect-with-drivers (qp.test/non-timeseries-drivers-with-feature :right-join)
  [[nil              "Fillmore Flock"]
   ["Big Red"        "Bayview Brood"]
   ["Callie Crow"    "Mission Street Murder"]
   ["Carson Crow"    "Mission Street Murder"]
   ["Chicken Little" "Bayview Brood"]
   ["Gerald Goose"   "Green Street Gaggle"]
   ["Greg Goose"     "Green Street Gaggle"]
   ["McNugget"       "Bayview Brood"]
   ["Oliver Owl"     "Portrero Hill Parliament"]
   ["Orville Owl"    "Portrero Hill Parliament"]
   ["Paul Pelican"   "SoMa Squadron"]
   ["Peter Pelican"  "SoMa Squadron"]
   ["Russell Crow"   "Mission Street Murder"]]
  (qp.test/rows
    (qp/process-query
      (query-with-strategy :right-join))))

;; Can we do an inner join?
(datasets/expect-with-drivers (qp.test/non-timeseries-drivers-with-feature :inner-join)
  [["Big Red"        "Bayview Brood"]
   ["Callie Crow"    "Mission Street Murder"]
   ["Carson Crow"    "Mission Street Murder"]
   ["Chicken Little" "Bayview Brood"]
   ["Gerald Goose"   "Green Street Gaggle"]
   ["Greg Goose"     "Green Street Gaggle"]
   ["McNugget"       "Bayview Brood"]
   ["Oliver Owl"     "Portrero Hill Parliament"]
   ["Orville Owl"    "Portrero Hill Parliament"]
   ["Paul Pelican"   "SoMa Squadron"]
   ["Peter Pelican"  "SoMa Squadron"]
   ["Russell Crow"   "Mission Street Murder"]]
  (qp.test/rows
    (qp/process-query
      (query-with-strategy :inner-join))))

;; Can we do a full join?
(datasets/expect-with-drivers (qp.test/non-timeseries-drivers-with-feature :full-join)
  [["Big Red"          "Bayview Brood"]
   ["Callie Crow"      "Mission Street Murder"]
   ["Camellia Crow"    nil]
   ["Carson Crow"      "Mission Street Murder"]
   ["Chicken Little"   "Bayview Brood"]
   ["Geoff Goose"      nil]
   ["Gerald Goose"     "Green Street Gaggle"]
   ["Greg Goose"       "Green Street Gaggle"]
   ["McNugget"         "Bayview Brood"]
   ["Olita Owl"        nil]
   ["Oliver Owl"       "Portrero Hill Parliament"]
   ["Orville Owl"      "Portrero Hill Parliament"]
   ["Oswald Owl"       nil]
   ["Pamela Pelican"   nil]
   ["Patricia Pelican" nil]
   ["Paul Pelican"     "SoMa Squadron"]
   ["Peter Pelican"    "SoMa Squadron"]
   ["Russell Crow"     "Mission Street Murder"]
   [nil                "Fillmore Flock"]]
  (qp.test/rows
    (qp/process-query
      (query-with-strategy :full-join))))

;; TODO Can we automatically include `:all` Fields?
(defn- x []
  (qp.test/rows+column-names
    (qp/process-query
      (data/dataset bird-flocks
        (data/mbql-query bird
          {:joins    [{:source-table $$flock
                       :condition    [:= [:field-id $flock_id] [:joined-field "f" [:field-id $flock.id]]]
                       :alias        "f"
                       :fields       :all}]
           :order-by [[:asc [:field-id $name]]]})))))

;; TODO Can we include no Fields (with `:none`)?

;; TODO Can we include a list of specific Fields?

;; TODO Can we join on a custom condition?

;; TODO Can we join on bucketed datetimes?

;; TODO Can we join against a source nested MBQL query?

;; TODO Can we join against a source nested native query?

;; TODO Can we include a list of specific Field for the source nested query?

;; TODO Do joins inside nested queries work?

;; TODO Can we join the same table twice with different conditions?

;; TODO Can we join the same table twice with the same condition?

;; TODO - Can we run a wacko query that does duplicate joins against the same table?
#_(defn- x []
  (data/mbql-query checkins
    {:source-query {:source-table $$checkins
                    :aggregation  [[:sum $user_id->users.id]]
                    :breakout     [[:field-id $id]]}
     :joins        [{:alias        "u"
                     :source-table $$users
                     :condition    [:=
                                    [:field-literal "ID" :type/BigInteger]
                                    [:joined-field "u" $users.id]]}]
     :limit        10}))
