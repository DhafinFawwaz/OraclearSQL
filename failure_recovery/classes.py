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

        self.storage_file = "log.json"
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


    # TODO: Use custom file format instead of JSON so we can easily append to the file. Also handle datetime serialization. but thats a job for query processor
    def __save_checkpoint(self) -> None:
        """Save a checkpoint in log. All entries in the write-ahead log from the last checkpoint are used to update data in physical storage in order to synchronize data. This method can be called after certain time periods (e.g. 5 minutes), and/or when the write-ahead log is almost full."""
        
        # remove ] from the beginning of the file and add , at the beginning of the current log
        # with open(self.storage_file, 'w') as file:
        #     to_write = str([log.__dict__ for log in self.write_ahead_log])
        #     to_write = to_write[1:]
        #     json.dump(to_write, file)

        with open(self.storage_file, 'a') as file:
            to_write = []
            for log in self.write_ahead_log:
                el = log.__dict__
                el["timestamp"] = str(el["timestamp"])
                # el["data"] = f"{el['data']}"
                el["data"] = f""
                to_write.append(el)
            to_write = str(to_write).replace("'", "\"")
            file.write(to_write)
            
        self.write_ahead_log.clear()
        self.last_checkpoint_time = datetime.now()



