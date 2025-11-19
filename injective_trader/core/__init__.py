"""
Core framework components for the Injective Trading Framework.

This module contains the foundational components including component lifecycle,
mediator pattern, state management, and task coordination.
"""

from .component import Component, Manager
from .mediator import Mediator
from .state import State, IdleState, RunningState, TerminatedState
from .task_manager import TaskManager

__all__ = [
    'Component',
    'Manager', 
    'Mediator',
    'State',
    'IdleState',
    'RunningState',
    'TerminatedState',
    'TaskManager',
]