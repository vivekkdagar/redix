from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """Server configuration container."""
    host: str = "localhost"
    port: int = 6379
    is_replica: bool = False
    master_host: Optional[str] = None
    master_port: Optional[int] = None
    dir: Optional[str] = None
    db_filename: Optional[str] = None
