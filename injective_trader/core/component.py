##########################
## Component Base Class ##
##########################

import asyncio
from typing import Dict, Optional, TYPE_CHECKING
from abc import ABC, abstractmethod

from pyinjective import AsyncClient
from pyinjective.core.network import Network
from pyinjective.composer import Composer

from injective_trader.core.state import State, IdleState, RunningState, TerminatedState
from injective_trader.utils.enums import Event
if TYPE_CHECKING:
    from injective_trader.core.mediator import Mediator

class Manager(ABC):
    def __init__(self, logger):
        self.logger = logger
        self.name = ""

    def set_mediator(self, mediator: "Mediator"):
        self.mediator = mediator

    @abstractmethod
    async def receive(self, event: Event, data: Dict):
        pass


class Component(Manager):
    def __init__(self, logger, composer_base):
        super().__init__(logger)
        self.state: Optional[State] = None
        self.mediator: Optional[Mediator] = None
        self.task: Optional[asyncio.Task] = None
        self.composer_base = composer_base

        self.network: Network = composer_base["network"]
        self.client: AsyncClient = composer_base["client"]
        self.composer: Composer = composer_base["composer"]

    async def set_state(self, state: State, **kwargs):
        self.state = state
        await self.state.execute(self, **kwargs)

    @abstractmethod
    async def initialize(self, **kwargs):
        pass

    @abstractmethod
    async def run(self, **kwargs):
        pass

    def start(self, **kwargs):
        task_name = self.name + "Task"
        self.task = asyncio.create_task(self._run_wrapper(**kwargs), name=task_name)

    async def _run_wrapper(self, **kwargs):
        running_state_args = kwargs.get("running_state_args", {})
        terminated_state_args = kwargs.get("terminated_state_args", {})

        await self.set_state(RunningState(), **running_state_args)
        try:
            await self.run(**running_state_args)
        finally:
            await self.set_state(TerminatedState(), **terminated_state_args)

    async def terminate(self, msg, **kwargs):
        """
        Cancel tasks from inside
        """
        terminated_state_args = kwargs.get("terminated_state_args", {})
        self.logger.info(f"Terminating {self.name} due to {msg}")
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                self.logger.info(f"{self.name} task was cancelled.")
        await self.set_state(TerminatedState(), **terminated_state_args)

    async def restart(self, **kwargs):
        """
        Restart tasks from inside: First initialize then run the task
        """
        idle_state_args = kwargs.get("idle_state_args", {})
        start_args = {
            "running_state_args": kwargs.get("running_state_args", {}),
            "terminated_state_args": kwargs.get("terminated_state_args", {}),
        }
        self.logger.info(f"Restarting {self.name}")
        await self.set_state(IdleState(), **idle_state_args)
        self.start(**start_args)
