# de_school_pokedata

Final project for Quantori Data enineering school 2022
<br>

## Project structure

<br>

Project uses following technologies and resources:
- PokéAPI v2 (https://pokeapi.co)
- Python and MWAA for orchestration
- S3 for staging file storage
- Snowpipe to transfer data to DWH
- Snowflake as DWH host
- DWH uses some kind of galaxy schema

<br>

### Directory structure:

<br>

```
.
├── LICENSE
├── README.md
├── scripts
│   └── snowflake_script.sql
├── src
│   └── airflow_dags
│       ├── degtyarev_daily_generetions_check.py
│       ├── degtyarev_load_pokemon_data.py
│       └── degtyarev_util
│           ├── __init__.py
│           ├── __pycache__
│           ├── control.py
│           ├── poke_api.py
│           ├── processing.py
│           └── s3_util.py
└── test

```

- `src/scripts` folder contains script for Snowflake DWH setup
- `src/airflow_dags` dir is for Airflow (MWAA) control DAGs code and helper-modules
- `test` is for unit tests
<br>

## Setup
- Upload content (subfolders structure should be preserved) of `src/airflow_dags` folder to MWAA DAGs S3 bucket
- Use `scripts/snowflake_script.sql` to set up Snowflake DWH, you will need to fill in the correct S3 credentials instead of "**"

<br>

## Data marts preview

<br>

### Pokémon count by types

| TYPE | POKEMON_COUNT | NEXT_DIFF | PREV_DIFF |
| :--- | :---: | :---: | :---: |
|unknown|0|0||
|shadow|0|58|0|
|ice|58|14|58|
|fairy|72|1|14|
|ghost|73|4|1|
|steel|77|1|4|
|dark|78|0|1|
|fighting|78|1|0|
|dragon|79|4|1|
|ground|83|2|4|

<br>

### Pokémon count by moves

|MOVE|POKEMON_COUNT|NEXT_DIFF|PREV_DIFF|
| :--- | :---: | :---: | :---: |
|protect|1 070|0||
|snore|1 070|2|0|
|substitute|1 068|1|2|
|rest|1 067|2|1|
|round|1 065|1|2|
|sleep-talk|1 064|3|1|
|facade|1 061|107|3|
|swagger|954|4|107|
|toxic|950|6|4|
|confide|944|0|6|

<br>

### Pokémon rating by the sum of stats

|POKEMON|STATS|
|:---|:---:|
|eternatus-eternamax|1 125|
|mewtwo-mega-x|780|
|mewtwo-mega-y|780|
|rayquaza-mega|780|
|groudon-primal|770|
|kyogre-primal|770|
|necrozma-ultra|754|
|arceus|720|
|zamazenta-crowned|720|
|zacian-crowned|720|

<br>

### Pokémon count by types and generations

|TYPE|GENERATION-I|GENERATION-II|GENERATION-III|GENERATION-IV|GENERATION-V|GENERATION-VI|GENERATION-VII|GENERATION-VIII|
|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
|bug|15|12|14|11|18|3|14|9|
|dark|0|17|15|7|16|8|2|13|
|dragon|5|2|14|8|12|14|9|15|
|electric|29|11|5|12|17|3|9|13|
|fairy|0|0|0|0|0|44|17|11|
|fighting|12|4|9|10|17|4|11|11|
|fire|17|11|10|5|18|8|9|10|
|flying|27|20|14|15|22|10|24|7|
|ghost|7|2|8|14|10|15|13|9|
|grass|17|10|18|16|21|17|14|14|