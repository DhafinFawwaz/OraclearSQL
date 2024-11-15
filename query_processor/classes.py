from datetime import datetime
from typing import List, Union, Generic, TypeVar

T = TypeVar("T")

class Rows(Generic[T]):
    def __init__(self, data: List[T]):
        self.data = data
        self.rows_count = len(data)
    
    def __str__(self):
        return str(self.__dict__)

class ExecutionResult:
    def __init__(self, transaction_id: int, timestamp: datetime, message: str, data: Union[Rows, int], query: str):
        self.transaction_id = transaction_id
        self.timestamp = timestamp
        self.message = message
        self.data = data
        self.query = query
    
    def __str__(self):
        return str(self.__dict__)