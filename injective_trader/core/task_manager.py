#####################
## Task Management ##
#####################

import asyncio
import traceback
import signal
from typing import Dict, Tuple
from injective_trader.core.component import Component
from injective_trader.core.state import IdleState, TerminatedState


class TaskManager:
    """
    Centralized management of tasks
    """

    def __init__(self, logger):
        self.logger = logger
        self.tasks: Dict[asyncio.Task, Tuple[str, None | Component, Dict]] = {}
        # NOTE: Flag 'running' is used to make sure the priority of signal handler
        # which should be higher than normal tasks monitoring
        self.running = True

    def add_task(self, component: Component, **kwargs):
        name = f"{component.name}Task"
        start_args = {
            "running_state_args": kwargs.get("running_state_args", {}),
            "terminated_state_args": kwargs.get("terminated_state_args", {}),
        }
        component.start(**start_args)
        if component.task is not None:
            self.tasks[component.task] = (name, component, kwargs)
        else:
            self.logger.warning(f"Cannot add task for component {name} - task is None")

    def add_task_noncomponent(self, func, name, **kwargs):
        task = asyncio.create_task(func(), name=name)
        self.tasks[task] = (name, None, kwargs)

    async def run(self):
        """
        Regular Tasks Monitoring
        """
        self.logger.info("TaskManager started running")
        self.logger.info(f"Active tasks when starting: {[name for name, _, _ in self.tasks.values()]}")
        while self.tasks:
            self.logger.info(f"Current active tasks: {[name for name, _, _ in self.tasks.values()]}")
            done, pending = await asyncio.wait(self.tasks.keys(), return_when=asyncio.FIRST_EXCEPTION)
            for task in done:
                if self.running:
                    name, component, kwargs = self.tasks.pop(task)
                    self.logger.info(f"Task {name} completed")
                    if task.exception() is not None:
                        self.log_exception(task, name)
                        if component:
                            terminated_state_args = kwargs.get("terminated_state_args", {})
                            idle_state_args = kwargs.get("idle_state_args", {})
                            await component.set_state(TerminatedState(), **terminated_state_args)
                            await component.set_state(IdleState(), **idle_state_args)
                            self.add_task(component, **kwargs)
                        else:
                            self.add_task_noncomponent(kwargs["runnable"], name, **kwargs)
                    else:
                        result = task.result()
                        self.logger.info(f"Task {name} completed successfully with result: {result}")

    def log_exception(self, task, name):
        tb = traceback.extract_tb(task.exception().__traceback__)
        filename, line_number, function_name, text = tb[-1]

        self.logger.error(f"Exception occurred in task {name}: {task.exception()}")
        self.logger.error(f"Exception type: {type(task.exception()).__name__}")
        self.logger.error(f"File: {filename}")
        self.logger.error(f"Function: {function_name}")
        self.logger.error(f"Line: {line_number}")
        self.logger.error(f"Code: {text}")
        self.logger.error("Traceback details:")
        self.logger.error(traceback.format_exc())

    async def shutdown(self, msg):
        self.running = False
        for task in list(self.tasks.keys()):
            name, component, kwargs = self.tasks.pop(task)
            if component:
                terminated_state_args = kwargs.get("terminated_state_args", {})
                await component.terminate(msg=msg, **terminated_state_args)
            else:
                task.cancel()
        self.logger.info("All tasks terminated during shutdown")

    async def signal_handler(self):
        self.logger.info("Received custom termination signal. Shutting down...")
        self.running = False
        self.logger.info("Terminating all tasks...")
        await self.shutdown(msg="SIGINT received")
        self.logger.info("All tasks terminated. Exiting...")

    def setup_signal_handler(self, loop):
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(self.signal_handler()))
