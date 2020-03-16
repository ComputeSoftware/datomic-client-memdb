#!/bin/bash

set -euo pipefail

clojure -Spom
clojure -A:jar