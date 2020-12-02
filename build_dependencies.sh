#!/usr/bin/env bash

# uncomment the freeze command to create a new requirements.txt file
#pip freeze > requirements.txt
pip install -r requirements.txt -t libs

zip -r jobs.zip dependencies jobs tests
cd libs && zip -r ../libs.zip . && cd ..


exit 0
