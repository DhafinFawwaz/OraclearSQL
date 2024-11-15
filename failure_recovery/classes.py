from datetime import datetime, timedelta
from typing import Optional, List, Callable
import json

import sys
from pathlib import Path
parent_folder = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_folder))
from query_processor.classes import ExecutionResult, Rows


class RecoverCriteria:
    def __init__(self, timestamp: Optional[datetime] = None, transaction_id: Optional[int] = None):
        self.timestamp = timestamp
        self.transaction_id = transaction_id

class FailureRecovery:
    def __init__(self):
        self.write_ahead_log: List[ExecutionResult] = []

        self.max_log_entries = 100
        self.max_checkpoint_time = 5 # minutes
        self.last_checkpoint_time = datetime.now()

        self.storage_file = "recovery.log"
        self.on_log_recovered: Callable[[ExecutionResult], None] = lambda log: print(log)


    def write_log(self, info: ExecutionResult) -> None:
        """Appends an entry in a write-ahead log based on execution info object."""

        self.write_ahead_log.append(info)
        
        # or/and
        if len(self.write_ahead_log) >= self.max_log_entries or datetime.now() - self.last_checkpoint_time >= timedelta(minutes=self.max_checkpoint_time):
            self.__save_checkpoint()


    def recover(self, criteria: RecoverCriteria) -> None:
        """Recovers the database to the state specified by the recovery criteria."""

        if criteria.timestamp:
            for log in reversed(self.write_ahead_log):
                if log.timestamp < criteria.timestamp: break
                self.on_log_recovered(log) # handled by query processor
        else:
            for log in reversed(self.write_ahead_log):
                if log.transaction_id < criteria.transaction_id: break
                self.on_log_recovered(log) # handled by query processor


    def __to_writable(self, log: ExecutionResult) -> str:
        res = str(log.transaction_id) + "\n"
        res += str(log.timestamp) + "\n"
        res += log.message + "\n"
        res += log.query + "\n"
        res += str(log.data) + "\n"
        return res
    
    def __from_writable(self, log: str) -> ExecutionResult:
        lines = log.split("\n")
        return ExecutionResult(
            transaction_id=int(lines[0]),
            timestamp=datetime.strptime(lines[1], "%Y-%m-%d %H:%M:%S.%f"),
            message=lines[2],
            data=Rows(data=[int(x) for x in lines[4].split(",")]),
            query=lines[3]
        )

    def __save_checkpoint(self) -> None:
        """Save a checkpoint in log. All entries in the write-ahead log from the last checkpoint are used to update data in physical storage in order to synchronize data. This method can be called after certain time periods (e.g. 5 minutes), and/or when the write-ahead log is almost full."""
        
        with open(self.storage_file, 'a') as file:
            to_write = ""
            for log in self.write_ahead_log:
                to_write += self.__to_writable(log)
            file.write(to_write)
            
        self.write_ahead_log.clear()
        self.last_checkpoint_time = datetime.now()



