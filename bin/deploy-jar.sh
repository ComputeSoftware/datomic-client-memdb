#!/bin/bash

mvn deploy:deploy-file -Dfile=datomic-client-memdb.jar -DrepositoryId=clojars -Durl=https://clojars.org/repo -DpomFile=pom.xml
rm datomic-client-memdb.jar