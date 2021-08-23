#!/bin/bash

apt-get install clang-format -y

git diff --name-only | egrep '.*\.(h|cc|cpp|inl)' | xargs -r clang-format -style=file -i
