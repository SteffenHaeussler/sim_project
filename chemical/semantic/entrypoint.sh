#!/bin/bash
ray start --head
serve start --http-host 0.0.0.0 --http-port 8000
serve run semantic.py
