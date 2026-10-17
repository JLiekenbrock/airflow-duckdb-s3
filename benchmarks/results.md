# Local dbt insert strategy benchmark

Source rows: 3,475,204; repeats: 3; max_threads=4; max_insert_threads=1.

Insert settings: async_insert=1, wait_for_async_insert=1, lightweight_deletes_sync=2 (matching the measured server defaults, pinned in the benchmark profile). This is not an async-on/off comparison.

| Strategy | Phase | Median dbt wall s | Median SQL ms | Max query MiB | Correct trials |
| --- | --- | ---: | ---: | ---: | ---: |
| append | initial | 2.524 | 347 | 153.3 | 3/3 |
| append | replay | 2.562 | 355 | 153.5 | 0/3 |
| append | partial_update | 2.271 | 84 | 57.0 | 0/3 |
| delete+insert | initial | 2.914 | 624 | 152.5 | 3/3 |
| delete+insert | replay | 3.420 | 1175 | 153.4 | 3/3 |
| delete+insert | partial_update | 2.558 | 304 | 57.1 | 3/3 |
| default | initial | 3.255 | 854 | 325.9 | 3/3 |
| default | replay | 3.471 | 1074 | 459.3 | 3/3 |
| default | partial_update | 2.711 | 441 | 213.4 | 3/3 |
| insert_overwrite | initial | 2.689 | 359 | 153.2 | 3/3 |
| insert_overwrite | replay | 2.609 | 368 | 153.2 | 3/3 |
| insert_overwrite | partial_update | 2.301 | 103 | 57.0 | 0/3 |

Fastest correct result per measured scenario (median dbt wall time):

- initial: `append` (2.524 s).
- replay: `insert_overwrite` (2.609 s).
- partial_update: `delete+insert` (2.558 s).

Initial = full source into an empty precreated table. Replay = identical full source over a full baseline. Partial update = first seven days over a full baseline, with user_id increased by one; rows outside that week must survive.

Correctness checks row count, exact distinct tenant/event count, and sum(user_id). Expected failures are benchmark outcomes, not Airflow execution failures. No fault injection or concurrent writers were tested.

Warm local caches; phases run sequentially with repeated source scans. Wall time includes dbt process startup and materialization; SQL ms sums tagged initial queries and excludes setup and validation. Peak memory is the largest individual measured query, not total server memory. Background merges and cache effects are not isolated. Results apply to this narrow NYC event projection and local versions only.
