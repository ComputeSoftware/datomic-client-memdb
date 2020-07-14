# Library is Deprecated 

The Datomic team released [dev-local](https://docs.datomic.com/cloud/dev-local.html) on July 10, 2020. This library has always been a temporary workaround for offline testing and easy CI support. Dev-local supersedes this library. I recommend switching over to dev-local.

# datomic-client-memdb

[![CircleCI](https://circleci.com/gh/ComputeSoftware/datomic-client-memdb.svg?style=svg)](https://circleci.com/gh/ComputeSoftware/datomic-client-memdb)

Datomic Client protocols for Datomic Peer databases.

## Installation

```clojure
datomic-client-memdb {:mvn/version "1.1.1"}
```

## Usage

Create a Client like you normally would with the Client API: 

```clojure
(require '[compute.datomic-client-memdb.core :as memdb])
(def c (memdb/client {}))
```

`memdb/client` returns `compute.datomic-client-memdb.core/Client` type which implements
`datomic.client.api/Client`. Calling `memdb/client` multiple times with the same
arg-map will result in the same instance of the client (i.e. the function is memoized).

This is technically the only function that this library exposes. All other operations
are done using the Datomic Client API. See [here](https://docs.datomic.com/client-api/datomic.client.api.html)
for a list of functions that you can call. 

### Suggested usage pattern

Our company has a database util library, and I imagine most companies have something
similar, that contains a number of functions associated with database operations.
Any time you use Datomic, that util library is probably on the classpath. I 
suggest adding a `client` function that returns a Datomic Client dependent on 
some values in the Datomic Client arg-map (the args passed to the `client` function).
For example,

```clojure
(defn client
  [datomic-config]
  (if (datomic-local? datomic-config)
    (do
      (require 'compute.datomic-client-memdb.core)
      (if-let [v (resolve 'compute.datomic-client-memdb.core/client)]
        (@v datomic-config)
        (throw (ex-info "compute.datomic-client-memdb.core is not on the classpath." {}))))
    (datomic.client.api/client datomic-config)))
```

The `datomic-local?` function returns true if `datomic-config` (the client arg-map)
has some particular value set. Perhaps `:local? true` or `:endpoint "localhost"`.

### Caveats

Datomic Cloud supports transaction functions through Datomic Ions. This library
does not have support for Ions.

This API wraps the Datomic Peer (On-Prem). There are features in Datomic Cloud's
query that are not present in Datomic On-Prem, and vice versa.

This library does not try to re-implement features from Datomic Cloud,
but makes some effort to throw exceptions when the user tries to use
features exclusive to on-prem, notably find spec for collections and scalars.

Because this is not exhaustive, it is recommended to also run integration tests
against an actual instance of Datomic Cloud.

### Cleanup

If you would like to cleanup the in-memory DBs, you can use the `close` function:

```clojure
(memdb/close c)
```

This will delete all the DBs associated with the client and purge them from the
client's DB store. Our Client also implements the `Closeable` interface so you can
use the Client with `with-open`, as seen [here](https://github.com/ComputeSoftware/datomic-client-memdb/blob/aa52ef9c125aef9c48777e0e3b024eb821f387a7/test/compute/datomic_client_memdb/core_test.clj#L10-L14).

Because this library is only intended to be used during the development and 
testing stages, you can probably just ignore the `close` function and only address 
cleanup if it becomes a problem.

## Implementation

This library is implemented by creating custom types (via `deftype`) that extend
the Datomic Client protocols located in the `datomic.client.api` namespace. I 
tried implementing this using `extend-type` on the Datomic peer classes (i.e. `datomic.db.Db`),
but you cannot add an implementation of `clojure.lang.ILookup` via `deftype` because
`ILookup` is an interface, not a protocol. We need to provide an implementation of
`ILookup` because that is how the Datomic Client API exposes `t` and `next-t`.

## License

Copyright © 2020 Compute Software

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
