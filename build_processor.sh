#! /usr/bin/env bash

docker build . --platform=linux/amd64 --tag icore_processor

docker save -o ./icore_processor.tar icore_processor
