#!/bin/sh

# Run Tests and Generate Coverage reports
coverage run --source=main -m pytest main_test.py
coverage html

# Run Python script
python ./main.py

# # Run tail command to keep the container alive
# tail -f /dev/null
