# Project Instructions

## Project Overview
- This repository contains local PySpark scripts for inspecting the Taobao `UserBehavior` CSV dataset.
- `scripts/00_count.py` counts rows and previews a few records; `scripts/01_profile.py` computes dataset quality and distribution metrics.
- `docs/data_profile.md` is the intended home for the profile report.
- The CSV is stored at `data/raw/UserBehavior.csv` and is excluded from Git. It is large; do not read, copy, regenerate, or commit it unless explicitly requested.

## Working Conventions
- Keep changes focused on the requested analysis or documentation. Preserve the existing Chinese explanations and output labels where practical.
- The CSV has no header and uses five fields: `user_id`, `item_id`, `category_id`, `behavior_type`, and `ts`. Keep the schema and field order consistent.
- Prefer paths derived from the script or repository location over machine-specific absolute paths when changing file access.
- Do not start a full Spark scan as an incidental check. Prefer syntax checks or small, explicitly bounded samples; run full-data jobs only when requested.
- Spark sessions should be stopped reliably, including when a job raises an exception.

## Validation
- There is no test suite or dependency manifest in the repository at present.
- For Python edits, run a syntax check and any available focused validation. Do not claim full-data results without actually running the corresponding Spark job.