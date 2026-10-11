# Roadmap — later layers

These files build on the `sql/` layers. They are kept separate from `sql/` on
purpose: everything in `sql/` was executed **with its measured results recorded
in the header comments**, and a file only moves across once that write-up is
done.

Each file is self-contained and depends only on the `sql/` layers above it.

| File | Covers | Status |
|---|---|---|
| `08_streams_tasks_merge.sql` | Incremental load: stream, scheduled task, MERGE upsert | Run, results not yet recorded |
| `09_feature_mart.sql` | Window-function feature layer | Run, results not yet recorded |
| `10_time_travel_clones.sql` | Time Travel recovery, zero-copy clones | Run, results not yet recorded |
| `11_masking.sql` | Dynamic data masking policy | Not yet run |
| `12_star_schema_scd2.sql` | Dimensional model + Slowly Changing Dimension Type 2 | Not yet run |
