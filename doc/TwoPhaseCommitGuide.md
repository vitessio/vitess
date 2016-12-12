# 2PC User guide

# Overview

Vitess 2PC allows you to perform atomic distributed commits. The feature is implemented using traditional MySQL transactions, and hence inherits the same guarantees.

The 2PC API itself is fairly simple: The Commit request accepts an additional `Atomic` flag. If it's set, then the 2PC mechanism is triggered. Without the flag, then a Commit is treated as best effort, which may result in partial commits if there are system failures during the operation.

2PC commits are more expensive than best effort because the system has to save away the statements before starting the commit process, and also clean them up after a successful commit. The application can make the trade-off call based on when it believes a 2PC commit is warranted. For single-database transactions, there is no additional overhead; 2PC commits and best effort commits are the same cost.
