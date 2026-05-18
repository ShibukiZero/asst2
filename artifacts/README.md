# Artifacts

This directory stores experiment outputs from the AWS grading environment.

## Layout

- `part_a/spawn/`: Part A Step 1 results for `TaskSystemParallelSpawn`.
- `part_a/spinning/`: Part A Step 2 results for `TaskSystemParallelThreadPoolSpinning`.
- `part_a/custom_tests/`: Custom synchronization edge-case tests used while preparing `TaskSystemParallelThreadPoolSleeping`.
- `part_a/sleeping/`: Part A Step 3 results for `TaskSystemParallelThreadPoolSleeping`.
- `part_b/`: Part B async dependency scheduler results, including the final async harness and custom dependency checks.

## Naming

- `red_*`: Baseline output before the implementation change.
- `green_*`: Output after the implementation change.
- Other files are focused sanity checks for the same implementation stage.
