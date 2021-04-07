#!/bin/bash

docker run -p 15000:15000 -p 15001:15001 -p 15991:15991 --rm -it vitess/local
