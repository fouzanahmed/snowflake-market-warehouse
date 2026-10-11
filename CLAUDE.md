# Agent Instructions for This Repository

You are operating autonomously, assigned exactly ONE backlog task. Follow these rules in order before making any change:

1. Read README.md fully before touching anything.
2. Inspect the repository structure (list directories, note the stack).
3. Run `git log --oneline -30` to see recent history — do not repeat work already done.
4. You will be given recent entries from this repo's automation history (prior categories/files touched). Never repeat the same category of change within 14 days unless the task explicitly says otherwise.
5. Check existing tests and CI config before adding new ones — extend, don't duplicate.
6. You have been assigned exactly one backlog item (given to you in the task prompt). Implement only that item. Do not expand scope, do not "also fix" unrelated things you notice — note them as follow-up ideas in your final structured output instead.
7. Preserve the project's actual purpose and existing architecture. Don't introduce a new framework/library/technology unless the task explicitly calls for it.
8. Keep the change small: 1-5 files, one logical commit's worth of work (a couple of commits' worth only if genuinely separate concerns, e.g. code + its test).
9. Test everything you change. Run the test suite, linter, and type checker (whichever exist in this repo) yourself before finishing. If you cannot get them passing, report failure — do not leave the working tree in a broken state.
10. NEVER fabricate results, metrics, benchmarks, or datasets. Never claim a feature exists if it doesn't. Any number you write into docs must be verifiable from real code/tests/data in this repo.
11. If the assigned task turns out to be already done, no longer applicable, or genuinely unsafe to do in one run, say so clearly in your structured output and stop — do not force an unrelated change to avoid "wasting the run."
12. Do not run `git add`, `git commit`, or `git push`, and do not open a PR. Leave your edits unstaged in the working tree — the orchestrator independently re-runs your tests, then handles staging, the commit, the push, and PR creation using the structured summary you provide at the end of your turn.

## Project-Specific Section
- **Goal:** A layered Snowflake warehouse over 11 years of daily S&P 500 prices (1,338,849 rows), built to measure Snowflake's ingestion, cost and pruning behavior hands-on. The README's numbers were all read off a live Snowflake account.
- **Architecture:**
  - `extract/fetch_prices.py` pulls the constituent list and OHLCV history and writes 10 shuffled gzipped CSVs.
  - `sql/01`–`07` were executed in order on a live account; measured results live in each file's header comment.
  - `roadmap/` holds later layers. Each header states its status (`08`–`10` run but not written up, `11`–`12` not yet run).
- **Technologies:** Snowflake SQL, SnowSQL, Python (pandas, yfinance).
- **Recruiter skills demonstrated:** Snowflake loading (stages, COPY INTO, lineage), idempotency testing, partition pruning and clustering decisions, RBAC and resource monitors, cost control.
- **Allowed improvement areas:** `extract/fetch_prices.py` code quality (typing, retries, logging), README/docs clarity, CI lint coverage.
- **Do not touch automatically:**
  - Any measured number in the README or in SQL header comments. These come from a live account and cannot be re-verified here.
  - The status tag in a `roadmap/` file header, and moving files between `roadmap/` and `sql/`. Only the owner does that, after pasting in real output.
  - SQL semantics in `sql/`. Nothing in this environment can run Snowflake, so SQL changes cannot be tested.
  - `.gitignore` entries for credentials and data.
