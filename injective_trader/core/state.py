####################
## State Patterns ##
####################

from abc import ABC, abstractmethod


class State(ABC):
    @abstractmethod
    async def execute(self, component, **kwargs):
        pass


class IdleState(State):
    """
    Task is initialized but not currently running.
    """

    async def execute(self, component, **kwargs):
        if hasattr(component, "initialize"):
            await component.initialize(**kwargs)
        component.logger.info(f"{component.name} is idle.")


class RunningState(State):
    """
    The event loop actually starts executing the task.
    """

    async def execute(self, component, **kwargs):
        component.logger.info(f"{component.name} is running.")
        await component.run(**kwargs)


class TerminatedState(State):
    """
    Task is terminated.
    """

    async def execute(self, component, **kwargs):
        if hasattr(component, "save_state"):
            component.save_state(**kwargs)
        component.logger.warning(f"{component.name} is terminated.")
