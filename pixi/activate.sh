#!/usr/bin/env bash

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PATH="$PROJECT_ROOT/tools/highs-1.14.0-x86_64-linux-gnu-static-apache/bin:$PATH"
