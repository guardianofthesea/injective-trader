import asyncio
import time
import logging
import datetime

class ThrottledCondition:
    def __init__(self, throttle_seconds:int, logger:logging.Logger):
        self.condition = asyncio.Condition()
        self.throttle_seconds = throttle_seconds
        self.last_notify_time = 0
        self.pending_notify = False
        self.logger=logger
        self.message = None  # Storage for the current message
        self.last_sent_message = None  # Storage for the last message that was actually sent

    async def notify(self, message:str|None):
        """Notify waiters
        at most once within throttle_seconds for same message
        imediately for diferent message
        """
        current_time = time.time()

        async with self.condition:
            if message != self.last_sent_message:
                self.logger.info("Message changed, sending notification immediately")
                self.condition.notify_all()
                self.last_notify_time = current_time
                self.pending_notify = False
                self.message = message
                self.last_sent_message = message  # Update the last sent message
            elif current_time - self.last_notify_time >= self.throttle_seconds:
                self.logger.info("Sent a notification")
                # More than throttle_seconds since last notification
                self.condition.notify_all()
                self.last_notify_time = current_time
                self.pending_notify = False
                self.message=message
                self.last_sent_message = message  # Update the last sent message
            else:
                self.logger.info(f"Ignored a notification until {datetime.datetime.fromtimestamp(self.last_notify_time+self.throttle_seconds)}")
                # Too soon, mark that a notification is pending
                self.pending_notify = True
                self.message=None

                # Schedule a delayed notification if needed
                if not hasattr(self, '_scheduled_task') or self._scheduled_task.done():
                    remaining = self.throttle_seconds - (current_time - self.last_notify_time)
                    self._scheduled_task = asyncio.create_task(self._delayed_notify(remaining, message))

    async def _delayed_notify(self, delay_seconds, message:str|None):
        """Helper to perform delayed notification"""
        await asyncio.sleep(delay_seconds)

        async with self.condition:
            if self.pending_notify:
                self.condition.notify_all()
                self.last_notify_time = time.time()
                self.pending_notify = False
                self.message = message
                self.last_sent_message = message  # Update the last sent message

    async def wait(self)->str|None:
        """Wait for notification"""
        self.logger.info("Waiting for notification")
        async with self.condition:
            await self.condition.wait()
            self.logger.info("Received a notification")
            return self.message
