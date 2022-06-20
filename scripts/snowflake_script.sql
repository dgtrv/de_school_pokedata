USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;
------------------------------------------------------------------------
-- using SYSADMIN role
------------------------------------------------------------------------
USE ROLE SYSADMIN;

------------------------------------------------------------------------
-- 1. creating warehouses
------------------------------------------------------------------------

-- deployment_wh for DB deployment
create warehouse if not exists tasks_wh
with warehouse_size=XSMALL max_cluster_count=1 initially_suspended=true;

-- and tasks_wh for continuous ETL
create warehouse if not exists deployment_wh
with warehouse_size=XSMALL max_cluster_count=1 initially_suspended=true;

use warehouse deployment_wh;

------------------------------------------------------------------------
-- 2. creating DB
------------------------------------------------------------------------
create database if not exists pokemon;

------------------------------------------------------------------------
-- 3. creating schemas
------------------------------------------------------------------------
use database pokemon;

-- Staging
create schema if not exists staging;

-- Result schema - experiments
create schema if not exists dwh;

-- Data marts schema
create schema if not exists data_marts;

------------------------------------------------------------------------
-- 4. Staging
------------------------------------------------------------------------
use schema pokemon.staging;


------------------------------------------------------------------------
-- 4.1. creating SF stage
------------------------------------------------------------------------
create stage if not exists source_data
  url='s3://de-school-snowflake/'
  credentials=(aws_key_id='**' aws_secret_key='**');

------------------------------------------------------------------------
-- 4.2. creating staging tables
------------------------------------------------------------------------

------------------------------------------------------------------------
-- 4.2.1. stg_pokemon
------------------------------------------------------------------------
create table if not exists stg_pokemon(json_data variant,
                           filename varchar,
                           amnd_user varchar default current_user(),
                           amnd_date datetime default current_timestamp()
                          );

------------------------------------------------------------------------
-- 4.2.2. stg_types
------------------------------------------------------------------------
create table if not exists stg_types(json_data variant,
                           filename varchar,
                           amnd_user varchar default current_user(),
                           amnd_date datetime default current_timestamp()
                          );

------------------------------------------------------------------------
-- 4.2.3. stg_moves
------------------------------------------------------------------------
create table if not exists stg_moves(json_data variant,
                           filename varchar,
                           amnd_user varchar default current_user(),
                           amnd_date datetime default current_timestamp()
                          );

------------------------------------------------------------------------
-- 4.2.4. stg_generations
------------------------------------------------------------------------
create table if not exists stg_generations(json_data variant,
                           filename varchar,
                           amnd_user varchar default current_user(),
                           amnd_date datetime default current_timestamp()
                          );

------------------------------------------------------------------------
-- 4.2.5. stg_pokemon_species
------------------------------------------------------------------------
create table if not exists stg_pokemon_species(json_data variant,
                                        filename varchar,
                                        amnd_user varchar default current_user(),
                                        amnd_date datetime default current_timestamp()
                                       );

------------------------------------------------------------------------
-- 4.3. creating pipelines
------------------------------------------------------------------------

------------------------------------------------------------------------
-- 4.3.0. creating file types
------------------------------------------------------------------------

-- JSON
create or replace file format JSON_DATA
type='JSON' STRIP_OUTER_ARRAY=TRUE;

------------------------------------------------------------------------
-- 4.3.1. stg_pokemon_pipe
------------------------------------------------------------------------
create pipe stg_pokemon_pipe auto_ingest=true as
    copy into stg_pokemon(json_data, filename)
    from
        (select $1, METADATA$FILENAME
         from @source_data/snowpipe/Degtyarev/pokemon/
         (file_format => JSON_DATA,
         pattern => '.*[.]json')
        );

------------------------------------------------------------------------
-- 4.3.2. stg_types_pipe
------------------------------------------------------------------------
create pipe stg_types_pipe auto_ingest=true as
    copy into stg_types(json_data, filename)
    from
        (select $1, METADATA$FILENAME
         from @source_data/snowpipe/Degtyarev/types/
         (file_format => JSON_DATA,
         pattern => '.*[.]json')
        );

------------------------------------------------------------------------
-- 4.3.3. stg_moves_pipe
------------------------------------------------------------------------
create pipe stg_moves_pipe auto_ingest=true as
    copy into stg_moves(json_data, filename)
    from
        (select $1, METADATA$FILENAME
         from @source_data/snowpipe/Degtyarev/moves/
         (file_format => JSON_DATA,
         pattern => '.*[.]json')
        );

------------------------------------------------------------------------
-- 4.3.4. stg_generations_pipe
------------------------------------------------------------------------
create pipe stg_generations_pipe auto_ingest=true as
    copy into stg_generations(json_data, filename)
    from
        (select $1, METADATA$FILENAME
         from @source_data/snowpipe/Degtyarev/generations/
         (file_format => JSON_DATA,
         pattern => '.*[.]json')
        );

------------------------------------------------------------------------
-- 4.3.5. stg_pokemon_species_pipe
------------------------------------------------------------------------
create pipe stg_pokemon_species_pipe auto_ingest=true as
    copy into stg_pokemon_species(json_data, filename)
    from
        (select $1, METADATA$FILENAME
         from @source_data/snowpipe/Degtyarev/pokemon_species/
         (file_format => JSON_DATA,
         pattern => '.*[.]json')
        );

------------------------------------------------------------------------
-- 4.4. creating streams
------------------------------------------------------------------------

------------------------------------------------------------------------
-- 4.4.1. stg_pokemon streams
------------------------------------------------------------------------
create stream stg_pokemon_stats_stream on table stg_pokemon;
create stream stg_pokemon_initial_types_stream on table stg_pokemon;
create stream stg_pokemon_types_history_stream on table stg_pokemon;

------------------------------------------------------------------------
-- 4.4.2. stg_types streams
------------------------------------------------------------------------
create stream stg_types_stream on table stg_types;
create stream stg_types_pokemon_stream on table stg_types;

------------------------------------------------------------------------
-- 4.4.3. stg_moves stream
------------------------------------------------------------------------
create stream stg_moves_pokemon_stream on table stg_moves;

------------------------------------------------------------------------
-- 4.4.4. stg_generations streams
------------------------------------------------------------------------
create stream stg_generations_stream on table stg_generations;
create stream stg_generations_psecies_stream on table stg_generations;
create stream stg_generations_types_stream on table stg_generations;

------------------------------------------------------------------------
-- 4.4.5. stg_pokemon_species stream
------------------------------------------------------------------------
create stream stg_pokemon_species_stream on table stg_pokemon_species;


------------------------------------------------------------------------
-- 5. DWH schema
------------------------------------------------------------------------
use schema pokemon.dwh;

------------------------------------------------------------------------
-- 5.1. creating tables
------------------------------------------------------------------------

------------------------------------------------------------------------
-- 5.1.1. types
------------------------------------------------------------------------
create table if not exists types(t_id number,
                                 t_type varchar
                                 );

------------------------------------------------------------------------
-- 5.1.2. types_pokemon
------------------------------------------------------------------------
create table if not exists types_pokemon(tp_id number autoincrement start 1 increment 1,
                                         tp_type varchar,
                                         tp_pokemon varchar
                                        );

------------------------------------------------------------------------
-- 5.1.3. moves_pokemon
------------------------------------------------------------------------
create table if not exists moves_pokemon(mp_id number autoincrement start 1 increment 1,
                                         mp_move varchar,
                                         mp_pokemon varchar
                                        );

------------------------------------------------------------------------
-- 5.1.4. pokemon_stats
------------------------------------------------------------------------
create table if not exists pokemon_stats(ps_id number autoincrement start 1 increment 1,
                                         ps_pokemon varchar,
                                         ps_stat varchar,
                                         ps_stat_value number
                                        );

------------------------------------------------------------------------
-- 5.1.5. species_pokemon
------------------------------------------------------------------------
create table if not exists species_pokemon(sp_id number autoincrement start 1 increment 1,
                                           sp_specie varchar,
                                           sp_pokemon varchar
                                          );

------------------------------------------------------------------------
-- 5.1.6. generations_species
------------------------------------------------------------------------
create table if not exists generations_species(gs_id number autoincrement start 1 increment 1,
                                               gs_generation_id number,
                                               gs_specie varchar
                                              );

------------------------------------------------------------------------
-- 5.1.7. pokemon_initial_types
------------------------------------------------------------------------
create table if not exists pokemon_initial_types(pit_id number autoincrement start 1 increment 1,
                                                 pit_pokemon varchar,
                                                 pit_initial_type varchar
                                                );

------------------------------------------------------------------------
-- 5.1.8. generations_types
------------------------------------------------------------------------
create table if not exists generations_types(gt_id number autoincrement start 1 increment 1,
                                             gt_generation_id number,
                                             gt_type varchar
                                            );

------------------------------------------------------------------------
-- 5.1.9. pokemon_types_history
------------------------------------------------------------------------
create table if not exists pokemon_types_history(pth_id number autoincrement start 1 increment 1,
                                                 pth_pokemon varchar,
                                                 pth_generation_name varchar,
                                                 pth_type varchar
                                                );

------------------------------------------------------------------------
-- 5.1.10. generations
------------------------------------------------------------------------
create table if not exists generations(g_id number,
                                       g_generation_name varchar
                                      );


------------------------------------------------------------------------
-- 6. Continuous ELT - tasks
------------------------------------------------------------------------
use schema pokemon.staging;

------------------------------------------------------------------------
-- 6.1. types
------------------------------------------------------------------------
create task if not exists moving_stg_types
    warehouse = 'tasks_wh'
    schedule = '5 minute'
    when system$stream_has_data('stg_types_stream')
    as
        insert into pokemon.dwh.types(t_id,
                                      t_type
                                     )
            select
                t.json_data:id::varchar as t_id,
                t.json_data:name::varchar as t_type
            from
                stg_types_stream t;

alter task moving_stg_types suspend;

------------------------------------------------------------------------
-- 6.2. types_pokemon
------------------------------------------------------------------------
create task if not exists moving_stg_types_pokemon
    warehouse = 'tasks_wh'
    schedule = '5 minute'
    when system$stream_has_data('stg_types_pokemon_stream')
    as
        insert into pokemon.dwh.types_pokemon(tp_type,
                                              tp_pokemon
                                             )
            select
                t.json_data:name::varchar as tp_type,
                f.value:pokemon:name::varchar as tp_pokemon
            from
                stg_types_pokemon_stream t,
                lateral flatten (input => t.json_data:pokemon) f;

alter task moving_stg_types_pokemon suspend;

------------------------------------------------------------------------
-- 6.3. moves_pokemon
------------------------------------------------------------------------
create task if not exists moving_stg_moves_pokemon
    warehouse = 'tasks_wh'
    schedule = '5 minute'
    when system$stream_has_data('stg_moves_pokemon_stream')
    as
        insert into pokemon.dwh.moves_pokemon(mp_move,
                                              mp_pokemon
                                             )
            select
                t.json_data:name::varchar as mp_move,
                f.value:name::varchar as mp_pokemon
            from
                stg_moves_pokemon_stream t,
                lateral flatten (input => t.json_data:learned_by_pokemon) f;

alter task moving_stg_moves_pokemon suspend;

------------------------------------------------------------------------
-- 6.4. pokemon_stats
------------------------------------------------------------------------
create task if not exists moving_stg_pokemon_stats
    warehouse = 'tasks_wh'
    schedule = '5 minute'
    when system$stream_has_data('stg_pokemon_stats_stream')
    as
        insert into pokemon.dwh.pokemon_stats(ps_pokemon,
                                              ps_stat,
                                              ps_stat_value
                                             )
            select
                t.json_data:name::varchar as ps_pokemon,
                f.value:stat:name::varchar as ps_stat,
                f.value:base_stat::number as ps_stat_value
            from
                stg_pokemon_stats_stream t,
                lateral flatten (input => t.json_data:stats) f;

alter task moving_stg_pokemon_stats suspend;

------------------------------------------------------------------------
-- 6.5. species_pokemon
------------------------------------------------------------------------
create task if not exists moving_stg_species_pokemon
    warehouse = 'tasks_wh'
    schedule = '5 minute'
    when system$stream_has_data('stg_pokemon_species_stream')
    as
        insert into pokemon.dwh.species_pokemon(sp_specie,
                                                sp_pokemon
                                               )
            select
                t.json_data:name::varchar as sp_specie,
                f.value:pokemon:name::varchar as sp_pokemon
            from
                stg_pokemon_species_stream t,
                lateral flatten (input => t.json_data:varieties) f;

alter task moving_stg_species_pokemon suspend;

------------------------------------------------------------------------
-- 6.6. generations_species
------------------------------------------------------------------------
create task if not exists moving_stg_generations_species
    warehouse = 'tasks_wh'
    schedule = '5 minute'
    when system$stream_has_data('stg_generations_psecies_stream')
    as
        insert into pokemon.dwh.generations_species(gs_generation_id,
                                                    gs_specie
                                                   )
            select
                t.json_data:id::number as gs_generation_id,
                f.value:name::varchar as gs_specie
            from
                stg_generations_psecies_stream t,
                lateral flatten (input => t.json_data:pokemon_species) f;

alter task moving_stg_generations_species suspend;

------------------------------------------------------------------------
-- 6.7. pokemon_initial_types
------------------------------------------------------------------------
create task if not exists moving_stg_pokemon_initial_types
    warehouse = 'tasks_wh'
    schedule = '5 minute'
    when system$stream_has_data('stg_pokemon_initial_types_stream')
    as
        insert into pokemon.dwh.pokemon_initial_types(pit_pokemon,
                                                      pit_initial_type
                                                     )
            select
                t.json_data:name::varchar as pit_pokemon,
                f.value:type:name::varchar as pit_initial_type
            from
                stg_pokemon_initial_types_stream t,
                lateral flatten (input => t.json_data:types) f;

alter task moving_stg_pokemon_initial_types suspend;

------------------------------------------------------------------------
-- 6.8. generations_types
------------------------------------------------------------------------
create task if not exists moving_stg_generations_types
    warehouse = 'tasks_wh'
    schedule = '5 minute'
    when system$stream_has_data('stg_generations_types_stream')
    as
        insert into pokemon.dwh.generations_types(gt_generation_id,
                                                  gt_type
                                                 )
            select
                t.json_data:id::number as gt_generation_id,
                f.value:name::varchar as gt_type
            from
                stg_generations_types_stream t,
                lateral flatten (input => t.json_data:types) f;

alter task moving_stg_generations_types suspend;

------------------------------------------------------------------------
-- 6.9. pokemon_types_history
------------------------------------------------------------------------
create task if not exists moving_stg_pokemon_types_history
    warehouse = 'tasks_wh'
    schedule = '5 minute'
    when system$stream_has_data('stg_pokemon_types_history_stream')
    as
        insert into pokemon.dwh.pokemon_types_history(pth_pokemon,
                                                      pth_generation_name,
                                                      pth_type
                                                     )
            select
                t.json_data:name::varchar as pth_pokemon,
                f.value:generation:name::varchar as pth_generation_name,
                f1.value:type:name::varchar as pth_type
            from
                stg_pokemon_types_history_stream t,
                lateral flatten (input => t.json_data:past_types) f,
                lateral flatten (input => f.value:types) f1;

alter task moving_stg_pokemon_types_history suspend;

------------------------------------------------------------------------
-- 6.10. moving_stg_generations
------------------------------------------------------------------------
create task if not exists moving_stg_generations
    warehouse = 'tasks_wh'
    schedule = '5 minute'
    when system$stream_has_data('stg_generations_stream')
    as
        insert into pokemon.dwh.generations(g_id,
                                            g_generation_name
                                           )
            select
                t.json_data:id::number as g_id,
                t.json_data:name::varchar as g_generation_name
            from
                stg_generations_stream t;

alter task moving_stg_generations suspend;


------------------------------------------------------------------------
-- 7. Data marts
------------------------------------------------------------------------
use schema pokemon.data_marts;

------------------------------------------------------------------------
-- 7.1. creating view v_pokemon_count_by_types
------------------------------------------------------------------------
create or replace view v_pokemon_count_by_types
as
    (select
        t_type as type,
        cnt as pokemon_count,
        next_diff,
        prev_diff
    from
        (select
            t_type,
            count(tp_pokemon) as cnt,
            lead(cnt) over(order by cnt) - cnt as next_diff,
            cnt - lag(cnt) over (order by cnt) as prev_diff
        from
            pokemon.dwh.types
            left join
            pokemon.dwh.types_pokemon
            on types.t_type=types_pokemon.tp_type
        group by t_type
        order by cnt, next_diff asc nulls last, prev_diff desc nulls first
        )
    );

------------------------------------------------------------------------
-- 7.2. creating view v_pokemon_count_by_moves
------------------------------------------------------------------------
create or replace view v_pokemon_count_by_moves
as
    (select
        mp_move as move,
        cnt as pokemon_count,
        next_diff,
        prev_diff
    from
        (select
            mp_move,
            count(mp_pokemon) as cnt,
            cnt - lead(cnt) over(order by cnt desc, mp_move) as next_diff,
            lag(cnt) over (order by cnt desc, mp_move) - cnt as prev_diff
        from
            pokemon.dwh.moves_pokemon
        group by mp_move
        order by cnt desc, next_diff asc nulls last, prev_diff desc nulls first
        )
    );

------------------------------------------------------------------------
-- 7.3. creating view v_pokemon_stats
------------------------------------------------------------------------
create or replace view v_pokemon_stats
as
    (select
        ps_pokemon as pokemon,
        sum(ps_stat_value) as stats
    from
        pokemon.dwh.pokemon_stats
    group by pokemon
    order by stats desc, pokemon
    );

------------------------------------------------------------------------
-- 7.3. creating view v_pokemon_by_types_and_generations
------------------------------------------------------------------------
create or replace view v_pokemon_by_types_and_generations
as
    (select type, sum("1") as "GENERATION-I",
        sum("2") as "GENERATION-II",
        sum("3") as "GENERATION-III",
        sum("4") as "GENERATION-IV",
        sum("5") as "GENERATION-V",
        sum("6") as "GENERATION-VI",
        sum("7") as "GENERATION-VII",
        sum("8") as "GENERATION-VIII"
    from
        ((select t_type as type, generation, pokemon
         from
            pokemon.dwh.types
            left join
            (select * from
                (select pit_pokemon as pokemon, max(greatest(gs_generation_id, gt_generation_id)) as generation
                 from
                    pokemon.dwh.species_pokemon
                    inner join
                    pokemon.dwh.generations_species
                    on species_pokemon.sp_specie=generations_species.gs_specie
                    inner join
                    pokemon.dwh.pokemon_initial_types
                    on species_pokemon.sp_pokemon=pokemon_initial_types.pit_pokemon
                    inner join
                    pokemon.dwh.generations_types
                    on pokemon_initial_types.pit_initial_type=generations_types.gt_type
                group by pit_pokemon
                ) pokemon_by_generations
                inner join
                pokemon.dwh.pokemon_initial_types
                on pokemon_by_generations.pokemon=pokemon_initial_types.pit_pokemon
            ) pokemon_by_generations_with_types
            on types.t_type = pokemon_by_generations_with_types.pit_initial_type
        )
        union
        (select pth_type as type, g_next_generation_id as generation, pth_pokemon as pokemon 
        from
            (select * from 
                pokemon.dwh.pokemon_types_history
                inner join
                pokemon.dwh.generations
                on pokemon_types_history.pth_generation_name=generations.g_generation_name) pokemon
            left join
            (select
                g_id,
                lead(g_id) over (order by g_id) as g_next_generation_id
            from pokemon.dwh.generations
            order by g_id) next_generations
            on pokemon.g_id=next_generations.g_id
        )
     )
        pivot(count(pokemon) for generation in (1, 2, 3, 4, 5, 6, 7, 8))
    group by type
    order by type
    );
