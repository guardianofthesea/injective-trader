from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from datetime import datetime, timedelta, timezone
import asyncio
from decimal import Decimal
import json
import os

@dataclass
class GranteeState:
    """Tracks state for each grantee"""
    address: str
    sequence: int
    last_used: datetime = field(default_factory=lambda: datetime.min.replace(tzinfo=timezone.utc))
    last_block_height: int = 0
    pending_txs: int = 0
    consecutive_errors: int = 0
    is_blocked: bool = False
    last_error: Optional[str] = None
    grants: Dict[str, datetime] = field(default_factory=dict)  # type -> expiration
    last_sync: datetime = field(default_factory=lambda: datetime.min.replace(tzinfo=timezone.utc))

class GranteePool:
    """Manages a pool of grantees for authz transactions"""

    def __init__(self, logger, granter_address: str, config: Dict):
        self.logger = logger
        self.granter_address = granter_address
        self.grantees: Dict[str, GranteeState] = {}
        self.current_block_height: int = 0
        self.grantees_used_in_block: Set[str] = set()
        self.current_index = 0
        self._lock = asyncio.Lock()

        # Configuration
        self.max_pending_txs = config.get("MaxPendingTxs", 5)
        self.error_threshold = config.get("ErrorThreshold", 3)
        self.block_duration = timedelta(seconds=config.get("BlockDuration", 300))
        self.rotation_interval = timedelta(seconds=config.get("RotationInterval", 1))
        self.sequence_sync_interval = timedelta(seconds=config.get("SequenceSyncInterval", 60))
        self.state_path = config.get("StatePath", f"grantee_pool_{granter_address}.json")

    async def initialize_grantees(self, grantee_configs: List[Dict]):
        """Initialize grantees from configuration"""
        for config in grantee_configs:
            grants = {
                grant["Type"]: datetime.fromisoformat(grant["Expiration"].replace('Z', '+00:00'))
                for grant in config.get("Grants", [])
            }

            await self.add_grantee(
                address=config["Address"],
                initial_sequence=config["InitialSequence"],
                grants=grants
            )

        # Load saved state
        await self._load_state()

    async def add_grantee(self, address: str, initial_sequence: int, grants: Dict[str, datetime]):
        """Add new grantee to the pool"""
        async with self._lock:
            self.grantees[address] = GranteeState(
                address=address,
                sequence=initial_sequence,
                last_used=datetime.min.replace(tzinfo=timezone.utc),
                grants=grants
            )
            self.logger.info(f"Added grantee {address} to pool with sequence {initial_sequence}")

    async def update_block_height(self, new_height: int):
        """Update current block height and reset block-specific tracking"""
        async with self._lock:
            if new_height > self.current_block_height:
                self.current_block_height = new_height
                self.grantees_used_in_block.clear()
                self.logger.info(f"New block height: {new_height}, reset grantee tracking")

    async def get_next_grantee(self, signing_grantees=None) -> Optional[GranteeState]:
        """Get next available grantee using round-robin with checks"""
        async with self._lock:
            if not self.grantees:
                return None

            start_index = self.current_index
            checked = 0
            now = datetime.now(timezone.utc)

            while checked < len(self.grantees):
                # Get next grantee
                addresses = list(self.grantees.keys())
                grantee_address = addresses[self.current_index]

                # Skip grantees not in signing_grantees if provided
                if signing_grantees is not None and grantee_address not in signing_grantees:
                    self.current_index = (self.current_index + 1) % len(self.grantees)
                    checked += 1
                    continue

                grantee = self.grantees[grantee_address]

                # Update index for next time
                self.current_index = (self.current_index + 1) % len(self.grantees)
                checked += 1

                # Check if grantee needs sequence sync
                if now - grantee.last_sync > self.sequence_sync_interval:
                    self.logger.debug(f"We need sequence sync")
                    await self._sync_grantee_sequence(grantee)

                # Check if grantee is available
                if self._is_grantee_available(grantee):
                    grantee.pending_txs += 1
                    grantee.last_used = now
                    return grantee
                else:
                    self.logger.debug(f"Skipping unavailable grantee {grantee.address}")

            self.logger.warning("No available grantees found")
            return None

    def _is_grantee_available(self, grantee: GranteeState) -> bool:
        """Check if grantee can be used"""
        now = datetime.now(timezone.utc)

        # Check if blocked
        if grantee.is_blocked:
            if now - grantee.last_used > self.block_duration:
                grantee.is_blocked = False
                grantee.consecutive_errors = 0
                self.logger.info(f"Unblocking grantee {grantee.address} after block duration")
            else:
                self.logger.debug(f"Grantee {grantee.address} is blocked")
                return False

        # Check pending transactions
        if grantee.pending_txs >= self.max_pending_txs:
            self.logger.debug(f"The number of the current grantee's pending txs {grantee.pending_txs} exceeds max_pending_txs {self.max_pending_txs}")
            return False

        # Check grants haven't expired
        if grantee.grants:
            for grant_type, expiration in grantee.grants.items():
                if expiration.tzinfo is None:
                    expiration = expiration.replace(tzinfo=datetime.timezone.utc)
                if now >= expiration:
                    self.logger.warning(f"Grant {grant_type} expired for {grantee.address}")
                    return False

        # Check rotation interval
        if now - grantee.last_used < self.rotation_interval:
            self.logger.debug(f"The grantee is picked up within rotational interval {self.rotation_interval}s: {now - grantee.last_used}")
            return False

        return True

    async def update_grantee_state(
        self,
        address: str,
        success: bool,
        new_sequence: Optional[int] = None,
        error: Optional[str] = None
    ):
        """Update grantee state after transaction"""
        async with self._lock:
            if address not in self.grantees:
                return

            grantee = self.grantees[address]
            grantee.pending_txs = max(0, grantee.pending_txs - 1)

            if success:
                grantee.consecutive_errors = 0
                if new_sequence is not None:
                    grantee.sequence = new_sequence
            else:
                grantee.consecutive_errors += 1
                grantee.last_error = error

                if new_sequence is not None:
                    grantee.sequence = new_sequence

                if grantee.consecutive_errors >= self.error_threshold:
                    grantee.is_blocked = True
                    grantee.last_used = datetime.now(timezone.utc)
                    self.logger.warning(
                        f"Blocking grantee {address} due to consecutive errors"
                    )

    async def refresh_grants(self, client) -> bool:
        """Refresh grant information for all grantees"""
        # {
        #     "grants":[
        #         {
        #             "authorization":"OrderedDict("[
        #                 "(""@type",
        #                 "/cosmos.authz.v1beta1.GenericAuthorization"")",
        #                 "(""msg",
        #                 "/injective.exchange.v1beta1.MsgCreateSpotLimitOrder"")"
        #             ]")",
        #             "expiration":"2024-12-07T02:26:01Z"
        #         }
        #     ]
        # }
        async with self._lock:
            try:
                for address, grantee in self.grantees.items():
                    try:
                        grants_reponse = await client.fetch_grants(
                            granter=self.granter_address,
                            grantee=address
                        )

                        # Parse grants from response
                        grants = {}
                        if "grants" in grants_reponse:
                            for grant in grants_reponse["grants"]:
                                auth = grant.get("authorization", {})
                                msg_type = None

                                # Extract message type from authorization
                                if isinstance(auth, dict):
                                    if "msg" in auth:
                                        msg_type = auth["msg"]
                                    elif "@type" in auth and "GenericAuthorization" in auth["@type"]:
                                        msg_type = auth.get("msg")

                                if msg_type and "expiration" in grant:
                                    expiration = datetime.fromisoformat(
                                        grant["expiration"].replace('Z', '+00:00')
                                    )
                                    grants[msg_type] = expiration

                        grantee.grants = grants

                    except Exception as e:
                        self.logger.error(f"Error refreshing grants for {address}: {e}")

                await self._save_state()
                return True

            except Exception as e:
                self.logger.error(f"Failed to refresh grants: {e}")
                return False

    async def _sync_grantee_sequence(self, grantee: GranteeState):
        """Synchronize grantee's sequence with the chain"""
        ### TODO: sync grantee seq
        try:
            # In real implementation, you would fetch the account from chain
            # For example, using client.fetch_account(grantee.address)
            # Then update the sequence number

            # This is where you'd make the actual call:
            # account_info = await self.client.fetch_account(grantee.address)
            # new_sequence = account_info.get("sequence", grantee.sequence)
            # grantee.sequence = new_sequence

            # For now, just mark as synced without actually syncing
            grantee.last_sync = datetime.now(timezone.utc)
            self.logger.debug(f"Synced sequence for grantee {grantee.address}: {grantee.sequence}")

        except Exception as e:
            self.logger.error(f"Error syncing sequence for grantee {grantee.address}: {e}")

    async def _save_state(self):
        """Save grantee states to disk"""
        try:
            # Convert grantee states to serializable format
            states = {}
            for address, grantee in self.grantees.items():
                states[address] = {
                    "address": grantee.address,
                    "sequence": grantee.sequence,
                    "last_used": grantee.last_used.isoformat(),
                    "last_block_height": grantee.last_block_height,
                    "pending_txs": grantee.pending_txs,
                    "consecutive_errors": grantee.consecutive_errors,
                    "is_blocked": grantee.is_blocked,
                    "last_error": grantee.last_error,
                    "grants": {
                        grant_type: expiration.isoformat()
                        for grant_type, expiration in grantee.grants.items()
                    },
                    "last_sync": grantee.last_sync.isoformat()
                }

            # Save to file
            with open(self.state_path, 'w') as f:
                json.dump({
                    "granter": self.granter_address,
                    "current_index": self.current_index,
                    "current_block_height": self.current_block_height,
                    "grantees": states,
                    "saved_at": datetime.now(timezone.utc).isoformat()
                }, f, indent=2)

            self.logger.debug(f"Saved grantee states to {self.state_path}")

        except Exception as e:
            self.logger.error(f"Error saving grantee states: {e}")

    async def _load_state(self):
        """Load grantee states from disk if available"""
        try:
            if not os.path.exists(self.state_path):
                self.logger.info(f"No saved state found at {self.state_path}")
                return

            with open(self.state_path, 'r') as f:
                state = json.load(f)

            if state.get("granter") != self.granter_address:
                self.logger.warning(f"Saved state is for different granter, ignoring")
                return

            # Load saved state
            self.current_index = state.get("current_index", 0)
            self.current_block_height = state.get("current_block_height", 0)

            # Load grantee states
            for address, saved_state in state.get("grantees", {}).items():
                if address not in self.grantees:
                    self.logger.info(f"Skipping saved state for unknown grantee {address}")
                    continue

                grantee = self.grantees[address]

                # Update basic fields
                grantee.sequence = saved_state.get("sequence", grantee.sequence)
                grantee.last_used = datetime.fromisoformat(saved_state.get("last_sync", datetime.now(timezone.utc).isoformat()))
                grantee.last_block_height = saved_state.get("last_block_height", 0)
                grantee.pending_txs = saved_state.get("pending_txs", 0)
                grantee.consecutive_errors = saved_state.get("consecutive_errors", 0)
                grantee.is_blocked = saved_state.get("is_blocked", False)
                grantee.last_error = saved_state.get("last_error")
                grantee.last_sync = datetime.fromisoformat(saved_state.get("last_sync", datetime.now(timezone.utc).isoformat()))

                # Load grants if they exist
                if "grants" in saved_state:
                    grantee.grants = {
                        grant_type: datetime.fromisoformat(expiration)
                        for grant_type, expiration in saved_state["grants"].items()
                    }

            self.logger.info(f"Loaded saved state from {self.state_path}")

        except Exception as e:
            self.logger.error(f"Error loading saved state: {e}")

    def get_pool_status(self) -> Dict:
        """Get current pool status"""
        now = datetime.now(timezone.utc)
        return {
            "granter": self.granter_address,
            "total_grantees": len(self.grantees),
            "available_grantees": sum(
                1 for g in self.grantees.values()
                if self._is_grantee_available(g)
            ),
            "blocked_grantees": sum(
                1 for g in self.grantees.values()
                if g.is_blocked
            ),
            "total_pending_txs": sum(
                g.pending_txs for g in self.grantees.values()
            ),
            "expired_grants": sum(
                1 for g in self.grantees.values()
                for _, expiration in g.grants.items()
                if now >= expiration
            ),
            "expiring_soon": sum(
                1 for g in self.grantees.values()
                for _, expiration in g.grants.items()
                if now < expiration <= now + timedelta(days=7)
            )
        }
