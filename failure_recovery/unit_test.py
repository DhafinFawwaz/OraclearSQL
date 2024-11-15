from datetime import datetime
from classes import FailureRecovery, ExecutionResult, RecoverCriteria, Rows

failure_recovery = FailureRecovery()
failure_recovery.max_log_entries = 2
failure_recovery.on_log_recovered = lambda log: print(f"Recovering {log}")

failure_recovery.write_log(ExecutionResult(1, datetime.now(), "Insert row", Rows(["row1"]), "INSERT INTO table VALUES ('row1')"))
failure_recovery.write_log(ExecutionResult(2, datetime.now(), "Update row", 1, "UPDATE table SET value = 'updated' WHERE id = 1"))
failure_recovery.write_log(ExecutionResult(3, datetime.now(), "Delete row", 1, "DELETE FROM table WHERE id = 1"))

criteria = RecoverCriteria(transaction_id=2)
failure_recovery.recover(criteria)
