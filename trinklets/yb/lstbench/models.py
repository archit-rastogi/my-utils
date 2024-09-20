"""Abstct classess for LST benchmarking."""

import json
import logging
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

LOGGER = logging.getLogger(__name__)


class Status(Enum):
    NOT_YET_STARTED = 5
    RUNNING = 10
    FINISHED = 15
    ERROR = 20
    TIMED_OUT = 25
    ABORTED = 30


class WorkloadComponentType(Enum):
    TASK = 5
    SESSION = 10
    PHASE = 15
    WORKLOAD = 20


class TaskType(Enum):

    LOAD = "L"
    SINGLE_USER = "SU"
    DATA_MAINTENANCE = "DM"
    OPTIMIZE = "O"


@dataclass
class BaseModel:

    name: str
    uuid: UUID
    create_time: datetime
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    status: Status
    component_type: WorkloadComponentType
    meta_data: str
    error_msg: str


@dataclass
class BaseTask(BaseModel):

    task_type: TaskType


@dataclass
class Session(BaseModel):
    """Session is a sequence of task that represents a logical unit of work."""

    logical_work: List[BaseTask]


class Phase(BaseModel):
    """Phase is collection of session(s)."""


class Workload(BaseModel):
    """A workload is a sequence of phases."""


@dataclass
class RuntimeConfig:

    # sequential when 1, more than 1 implies parallel
    with_concurrency: int = 1
    timeout_secs: int = 900

# create a base class to work with sqlite3


class Sqlite3Base:

    def __init__(self, database: Optional[str] = None, db_path: Optional[Path] = None,
                 logger: Optional[logging.Logger] = None):
        self.database = database if database is not None else ":memory:"
        self.db_path: Path = db_path if db_path is not None else Path(".")
        self.logger = logger if logger is not None else logging.getLogger(__name__)
        self.timeout = 30

    def get_db_file_path(self) -> Path:
        if self.database == ":memory":
            return self.database
        return self.db_path.joinpath(self.database).absolute()

    @contextmanager
    def with_connection(self, isolation_level=None):
        conn = None
        try:
            conn = sqlite3.connect(
                database=str(self.get_db_file_path()),
                timeout=self.timeout,
                isolation_level=isolation_level
            )
            yield conn
            conn.commit()
        finally:
            if conn:
                conn.close()

    @contextmanager
    def in_txn(self, conn: sqlite3.Connection):
        conn.isolation_level = None
        cur: sqlite3.Cursor = conn.cursor()
        cur.execute("BEGIN")
        try:
            yield cur
            cur.execute("COMMIT")
        except Exception as exc:
            LOGGER.error("Exception occured: %s", exc.args[0], exc_info=True)
            cur.execute("ROLLBACK")
        finally:
            if cur:
                cur.close()

    @contextmanager
    def with_cursor(self, conn=None):

        def _with_cursor(conn_1):
            cur: sqlite3.Cursor = None
            try:
                cur = conn_1.cursor()
                yield cur
            finally:
                if cur:
                    cur.close()

        if conn is None:
            with self.with_connection() as conn_0:
                yield from _with_cursor(conn_1=conn_0)
        else:
            yield from _with_cursor(conn_1=conn)

    def insert(self, cur: sqlite3.Cursor, table_name: str, record: Dict[str, str]):
        columns = ','.join(record.keys())
        placeholders = ",".join([f":{k}" for k in record.keys()])
        sql = f"""
            INSERT INTO {table_name}({columns})
            VALUES ({placeholders})
        """
        cur.execute(sql, record)


class Handler(Sqlite3Base):

    def __init__(self, database: Optional[str] = None, db_path: Optional[Path] = None):
        super().__init__(database, db_path)

    def create_tables_if_not_exists(self):
        script = Path("../src/main/lstbench/ddl.sql").absolute()
        LOGGER.info("Running ddl script at %s", script)
        with open(script, encoding="utf-8") as script_file:
            script_content = script_file.read()
            with self.with_connection() as conn, self.with_cursor(conn=conn) as cur:
                cur.executescript(script_content)

    def get_as_record(self, target: BaseModel) -> Dict[str, str]:
        column_value = {
            "name": target.name,
            "uuid": target.uuid,
            "create_time": target.create_time,
            "start_time": target.start_time,
            "end_time": target.end_time,
            "status": target.status.value,
            "component_type": target.component_type.value,
            "meta_data": target.meta_data,
            "error_msg": target.error_msg
        }
        return column_value

    def __create_base_args(self) -> Dict[str, Any]:
        return {
            "uuid": uuid4().bytes.hex(),
            "create_time": datetime.utcnow(),
            "start_time": None,
            "end_time": None,
            "status": Status.NOT_YET_STARTED,
            "meta_data": "",
            "error_msg": ""
        }

    def create_new_workload(self, name: str) -> Workload:
        new_workload = Workload(name=name, component_type=WorkloadComponentType.WORKLOAD, **self.__create_base_args())
        workload_record = self.get_as_record(target=new_workload)

        with self.with_cursor() as cur:
            self.insert(cur, table_name="workload", record=workload_record)
        return new_workload

    def create_new_phase(self, name: str) -> Phase:
        new_phase = Phase(name=name, component_type=WorkloadComponentType.PHASE, **self.__create_base_args())
        phase_record = self.get_as_record(target=new_phase)

        with self.with_cursor() as cur:
            self.insert(cur, table_name="phase", record=phase_record)
        return new_phase

    def create_new_session(self, name: str) -> Session:
        new_session = Session(
            name=name, component_type=WorkloadComponentType.SESSION, logical_work=[], **self.__create_base_args())
        session_record = self.get_as_record(target=new_session)

        with self.with_cursor() as cur:
            self.insert(cur, table_name="session", record=session_record)
        return new_session

    def create_new_task(self, name: str, task_type: TaskType) -> BaseTask:

        new_task = BaseTask(
            name=name,
            task_type=task_type,
            component_type=WorkloadComponentType.TASK,
            **self.__create_base_args()
        )

        task_record = self.get_as_record(target=new_task)
        task_record["task_type"] = task_type.value
        with self.with_cursor() as cur:
            self.insert(cur, table_name="base_task", record=task_record)
        return new_task

    def __start(self, cur, table_name: str, uuid: str, start_time: datetime):
        sql = f"""
            UPDATE {table_name}
            SET start_time=:start_time, status=:status WHERE uuid=:uuid
        """
        cur.execute(sql, {"start_time": start_time, "status": Status.RUNNING.value, "uuid": uuid})
        if cur.rowcount != 1:
            LOGGER.debug("%s update sql: %s", table_name, sql)
            LOGGER.error("Incorrectly Updated rows: %s", cur.rowcount)
            raise RuntimeError(f"Failed to start {table_name}: Updated {cur.rowcount} but expected 1!")

    def start_task(self, task: BaseTask, session: Session, meta_data: Dict[str, Any]):
        # map the task to the session
        session_task_rec = {
            "session_uuid": session.uuid,
            "task_uuid": task.uuid,
            "meta_data": self.dump_json(meta_data)
        }
        with self.with_connection() as conn, self.in_txn(conn=conn) as cur:
            # insert session <-> task mapping / session has one or more tasks
            self.insert(cur, table_name="session_tasks", record=session_task_rec)
            start_time = datetime.utcnow()
            self.__start(cur, "base_task", task.uuid, start_time)
            task.start_time = start_time
            task.status = Status.RUNNING

    def start_session(self, session: Session, phase: Phase, meta_data: Dict[str, Any]):
        # map the session to the phase
        phase_sessions_rec = {
            "phase_uuid": phase.uuid,
            "session_uuid": session.uuid,
            "meta_data": self.dump_json(meta_data)
        }
        with self.with_connection() as conn, self.in_txn(conn=conn) as cur:
            self.insert(cur, table_name="phase_sessions", record=phase_sessions_rec)
            start_time = datetime.utcnow()
            self.__start(cur, "session", session.uuid, start_time)
            session.start_time = start_time
            session.status = Status.RUNNING

    def start_phase(self, phase: Phase, workload: Workload, meta_data: Dict[str, Any]):
        # map the pahse to the workload
        workload_phases_rec = {
            "phase_uuid": phase.uuid,
            "workload_uuid": workload.uuid,
            "meta_data": self.dump_json(meta_data)
        }
        with self.with_connection() as conn, self.in_txn(conn=conn) as cur:
            self.insert(cur, table_name="workload_phases", record=workload_phases_rec)
            start_time = datetime.utcnow()
            self.__start(cur, "phase", phase.uuid, start_time)
            phase.start_time = start_time
            phase.status = Status.RUNNING

    def start_workload(self, workload: Workload):
        with self.with_connection() as conn, self.in_txn(conn=conn) as cur:
            start_time = datetime.utcnow()
            self.__start(cur, "workload", workload.uuid, start_time)
            workload.start_time = start_time
            workload.status = Status.RUNNING

    def __end(
            self, cur, table_name: str, uuid: str, status: Status, end_time: datetime, error_msg: Optional[str] = None):
        update_sql = f"""
                UPDATE {table_name}
                SET end_time=:end_time, status=:status WHERE uuid=:uuid
            """
        cur.execute(update_sql, {"end_time": end_time, "status": status.value, "uuid": uuid})
        if cur.rowcount != 1:
            LOGGER.debug("Task update sql: %s", update_sql)
            LOGGER.error("Incorrectly Updated rows: %s", cur.rowcount)
            raise RuntimeError(f"Failed to end {table_name}: Updated {cur.rowcount} but expected 1!")
        cur.connection.commit()

        if not error_msg:
            update_sql = f"""
                UPDATE {table_name}
                SET error_msg=:error_msg WHERE uuid=:uuid
            """
            cur.execute(update_sql, {"error_msg": error_msg, "uuid": uuid})
            cur.connection.commit()

    def end_task(self, task: BaseTask, status: Status, error_msg: Optional[str] = None):
        with self.with_connection() as conn, self.with_cursor(conn=conn) as cur:
            end_time = datetime.utcnow()
            self.__end(cur, "base_task", task.uuid, status, end_time, error_msg)
            task.end_time = end_time
            task.status = status
            task.error_msg = error_msg

    def end_session(self, session: Session, status: Status, error_msg: Optional[str] = None):
        with self.with_connection() as conn, self.with_cursor(conn=conn) as cur:
            end_time = datetime.utcnow()
            self.__end(cur, "session", session.uuid, status, end_time, error_msg)
            session.end_time = end_time
            session.status = status
            session.error_msg = error_msg

    def end_phase(self, phase: Phase, status: Status, error_msg: Optional[str] = None):
        with self.with_connection() as conn, self.with_cursor(conn=conn) as cur:
            end_time = datetime.utcnow()
            self.__end(cur, "phase", phase.uuid, status, end_time, error_msg)
            phase.end_time = end_time
            phase.status = status
            phase.error_msg = error_msg

    def end_workload(self, workload: Workload, status: Status, error_msg: Optional[str] = None):
        with self.with_connection() as conn, self.with_cursor(conn=conn) as cur:
            end_time = datetime.utcnow()
            self.__end(cur, "workload", workload.uuid, status, end_time, error_msg)
            workload.end_time = end_time
            workload.status = status
            workload.error_msg = error_msg

    def dump_json(self, content: Dict[str, Any]) -> str:
        return json.dumps(content, indent=None, sort_keys=True, separators=(',', ':'))
