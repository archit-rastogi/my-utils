import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from functools import partial
from random import choice
from typing import Any, Dict, Generator, Optional

from main.config import Config, HostConfig
from main.lstbench.models import (BaseTask, Handler, Phase, Session, Status,
                                  TaskType, Workload)
from main.report import Report, Step

LOGGER = logging.getLogger(__name__)

sqlite3_handler = Handler(database="test.db")


class BaseTaskRunnable(ABC):

    @abstractmethod
    def run(self, run_on_host: HostConfig):
        pass

    @abstractmethod
    def wait(self, timeout: Optional[float] = 0):
        pass


class LstTask(BaseTaskRunnable):

    def __init__(self, task_type: TaskType):
        self.task_type: TaskType = task_type


class WorkloadRunner:

    def __init__(self, config: Config):
        self.config = config
        self.reporter = self.config.meta.reporter

    def run_and_wait(self, task: LstTask, meta: Dict[str, str] = None):
        if not meta:
            meta = {}

        # get a random client
        run_on_host: HostConfig = None
        if self.config.client_hosts:
            run_on_host = choice(self.config.client_hosts)
        task.run(run_on_host=run_on_host)
        task.wait()


class ExperimentRunner:
    """An experiment is a workload run against a config."""

    def __init__(self, config: Config):
        self.config = config
        self.reporter: Report = self.config.meta.reporter

        # configure step
        self.step = partial(Step, reporter=self.reporter)

    @contextmanager
    def task_ctx(self, session: Session, name: str, task_type: TaskType) -> Generator[BaseTask, None, None]:
        task = sqlite3_handler.create_new_task(name, task_type)
        sqlite3_handler.start_task(task, session, {})

        status = Status.FINISHED
        error_msg = None
        with self.step(name=f"Task: {name}", properties={"task_type": task_type.name}) as step:
            try:
                yield task
            except Exception as exc:
                error_msg = exc.args[0]
                status = Status.ERROR

                step.failed()
                step.edit_step_properties({"exc": str(exc.args[0])})
                # fail the session if task has failed
                raise RuntimeError(f"Task {name} failed.") from exc
            finally:
                sqlite3_handler.end_task(task, status, error_msg)

    @contextmanager
    def session_ctx(self, phase: Phase, name: str) -> Generator[Session, None, None]:
        session = sqlite3_handler.create_new_session(name)
        sqlite3_handler.start_session(session, phase, {})

        status = Status.FINISHED
        error_msg = None
        with self.step(name=f"Session: {name}", properties={}) as step:
            try:
                yield session
            except Exception as exc:
                error_msg = exc.args[0]
                status = Status.ERROR

                step.failed()
                step.edit_step_properties({"exc": str(exc.args[0])})
                raise RuntimeError(f"Session {name} failed.") from exc
            finally:
                sqlite3_handler.end_session(session, status, error_msg)

    @contextmanager
    def phase_ctx(self, workload: Workload, name: str) -> Generator[Phase, None, None]:
        phase = sqlite3_handler.create_new_phase(name)
        sqlite3_handler.start_phase(phase, workload, {})

        status = Status.FINISHED
        error_msg = None
        with self.step(name=f"Phase: {name}", properties={}) as step:
            try:
                yield phase
            except Exception as exc:
                error_msg = exc.args[0]
                status = Status.ERROR

                step.failed()
                step.edit_step_properties({"exc": str(exc.args[0])})
                raise RuntimeError(f"Phase {name} failed.") from exc
            finally:
                sqlite3_handler.end_phase(phase, status, error_msg)

    @contextmanager
    def workload_ctx(self, name: str) -> Generator[Phase, None, None]:
        workload = sqlite3_handler.create_new_workload(name)
        sqlite3_handler.start_workload(workload)

        status = Status.FINISHED
        error_msg = None
        try:
            yield workload
        except Exception as exc:
            error_msg = exc.args[0]
            status = Status.ERROR
            raise RuntimeError(f"Workload {name} failed.") from exc
        finally:
            sqlite3_handler.end_workload(workload, status, error_msg)

    def run(self, workload_definition: Dict[str, Any]):
        sqlite3_handler.create_tables_if_not_exists()

        workload_instance = WorkloadRunner(config=self.config)
        with self.workload_ctx(workload_definition["name"]) as curr_workload:
            for phase_index, phase_def in enumerate(workload_definition["phases"]):
                with self.phase_ctx(curr_workload, phase_def["name"]) as curr_phase:
                    for session_index, session_def in enumerate(phase_def["sessions"]):
                        with self.session_ctx(curr_phase, session_def["name"]) as curr_session:
                            for task_index, task_def in enumerate(session_def["tasks"]):
                                task_instance: LstTask = task_def["task"]
                                task_type = task_instance.task_type
                                task_name = task_def.get("name", f"{task_type}_{task_instance.__class__.__name__}")
                                task_meta = {
                                    "phase_index": phase_index,
                                    "session_index": session_index,
                                    "task_index": task_index
                                }
                                with self.task_ctx(curr_session, task_name, task_type) as curr_task:
                                    task_meta["uuid"] = curr_task.uuid
                                    workload_instance.run_and_wait(task=task_instance, meta=task_meta)
                            LOGGER.info("All %d tasks executed", len(session_def["tasks"]))
                    LOGGER.info("All %d sessions finished", len(phase_def["sessions"]))
            LOGGER.info("All %d phases finished", len(workload_definition["phases"]))
