# Local dbt insert strategy benchmark

Source rows: 3,475,204; repeats: 3; max_threads=4; max_insert_threads=1.

Insert settings: async_insert=1, wait_for_async_insert=1, lightweight_deletes_sync=2 (matching the measured server defaults, pinned in the benchmark profile). This is not an async-on/off comparison.

| Strategy | Phase | Median dbt wall s | Median SQL ms | Max query MiB | Correct trials |
| --- | --- | ---: | ---: | ---: | ---: |
| append | initial | 3.317 | 1010 | 692.3 | 3/3 |
| append | replay | 3.340 | 1012 | 672.6 | 0/3 |
| append | partial_update | 2.509 | 211 | 223.5 | 0/3 |
| delete+insert | initial | 4.364 | 2030 | 688.1 | 3/3 |
| delete+insert | replay | 4.621 | 2265 | 689.7 | 3/3 |
| delete+insert | partial_update | 2.851 | 486 | 226.3 | 3/3 |
| default | initial | 4.494 | 2101 | 673.6 | 3/3 |
| default | replay | 4.561 | 2177 | 689.4 | 3/3 |
| default | partial_update | 3.753 | 1296 | 658.0 | 3/3 |
| insert_overwrite | initial | 3.371 | 1008 | 673.6 | 3/3 |
| insert_overwrite | replay | 3.434 | 1048 | 689.6 | 3/3 |
| insert_overwrite | partial_update | 2.644 | 227 | 223.5 | 0/3 |

Fastest correct result per measured scenario (median dbt wall time):

- initial: `append` (3.317 s).
- replay: `insert_overwrite` (3.434 s).
- partial_update: `delete+insert` (2.851 s).

Initial = full source into an empty precreated table. Replay = identical full source over a full baseline. Partial update = first seven days over a full baseline, increasing each non-null total_amount by USD 1.00; other days must survive.

Correctness checks row count, exact distinct trip IDs, and total_amount summed as integer cents. Expected failures are benchmark outcomes, not Airflow execution failures. No fault injection or concurrent writers were tested.

Warm local caches; phases run sequentially with repeated source scans. Wall time includes dbt process startup and materialization; SQL ms sums tagged initial queries and excludes setup and validation. Peak memory is the largest individual measured query, not total server memory. Background merges and cache effects are not isolated. Results apply to the 24-column NYC taxi schema and local versions only.
