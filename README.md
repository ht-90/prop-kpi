# prop-kpi

## Requirements

* Python3.X
* Docker

## Setup

Python environment

```shell
>>> python3 -m venv .venv
>>> source .venv/bin/activate
>>> pip install -r requirements.txt
```

Create `.env` file at root directory:

```.env file
# AWS
AWS_ACCESS_KEY_ID = TEST
AWS_SECRET_ACCESS_KEY = TEST
AWS_DEFAULT_REGION = us-east-1

# Database
DB_ENDPOINT_URL = http://localhost:8000
```

