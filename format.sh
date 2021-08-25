#!/bin/bash

git diff --name-only | egrep '.*\.(h|cc|cpp|inl)' | xargs -r clang-format -i
