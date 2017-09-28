# Data Africa API

The Data Africa API enables access to a variety of data sources on agriculture, climate, health and poverty through a unified endpoint. For usage information, visit the [API wiki](https://github.com/Datawheel/data-africa-api/wiki) 

## Requirements

### Database
The Data Africa API uses Postgres 9.6 and the PostGIS and fuzzystrmatch extensions.

To install PostGIS use:

```
CREATE EXTENSION postgis;
```

To install the fuzzystrmatch extension you may run the follow command:
a
```
CREATE EXTENSION fuzzystrmatch;
```

## Installation

1. Clone the repository 
```git clone https://github.com/data-africa/data-africa-api.git```

2. (optional, but recommended) Create a virtual environment using `virtualenv` e.g. 
```virtualenv /path/to/Envs/data-africa-api```

3. Install requirements 
```pip install -r requirements.txt```

4. Set required environment variables

```
export DATA_AFRICA_DB_NAME=data_africa
export DATA_AFRICA_DB_USER=postgres
export DATA_AFRICA_DB_PW=yourpasswordgoeshere
export DATA_AFRICA_DB_HOST=127.0.0.1
export DATA_AFRICA_PRODUCTION=True
```

5. Test run

```python run.py```

6. For deployment we suggest using either supervisor or systemd to manage the gunicorn processes, starting with the following:

```
/path/to/Envs/data-africa-api/bin/gunicorn -w 4 data_africa:app -b 127.0.0.1:5000 --timeout 120
```
