# TechcraftingAI

# Fetch Daily arXiv Summaries

Lambda to fetch daily arXiv research summaries and save them to S3.

## Dependencies

psycopg2 has to be installed manually.

Needs pip install --platform=manylinux1_x86_64 --only-binary=:all: psycopg2-binary --target psycopg-binary/python/lib/python3.9/site-packages
Also needs 3.9 version from https://github.com/jkehler/awslambda-psycopg2/tree/master copies into /package, change the folder name to `psycopg2` and take out the version requirement in the requirements.txt