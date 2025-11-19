from typing import Dict, Optional
from pyinjective.core.token import Decimal
from pyinjective.wallet import Address
from pyinjective.wallet import PrivateKey, PublicKey
from injective_trader.domain.account.subaccount import SubAccount

class Account:
    def __init__(self, logger, account_address: str, 
                 address: Optional[Address] = None, 
                 private_key: Optional[PrivateKey] = None,
                 public_key: Optional[PublicKey] = None):
        self.logger = logger
        self.account_address: str = account_address
        
        # Required if this account is used for broadcasting, Otherwise optional
        self.private_key: Optional[PrivateKey] = private_key
        self.public_key: Optional[PublicKey] = public_key
        self.address: Optional[Address] = address

        self.bank_balances: Dict[str, BankBalance] = {}
        self.balances: Dict[str, Balance] = {}
        self.subaccounts: Dict[str, SubAccount] = {}
        self.is_readonly = private_key is None

    @property
    def sequence(self) -> int:
        if self.address:
            return self.address.sequence
        return 0
    
    @sequence.setter
    def sequence(self, value: int):
        if self.address:
            self.address.sequence = value
        else:
            self.logger.warning(f"Cannot set sequence for read-only account {self.account_address}")
    
    def can_sign_transaction(self) -> bool:
        """Check if account has the necessary credentials for transaction signing"""
        return not self.is_readonly and self.private_key is not None and self.address is not None

class Balance:
    def __init__(self, denom: str, total: str, available: str) -> None:
        self.denom = denom
        self.total = Decimal(f"{total}")
        self.available = Decimal(f"{available}")

    def update(self, total: Decimal, available: Decimal, is_stream=False):
        if not is_stream:
            self.total = total
            self.available = available
        else:
            self.total = total
            self.available = available

class BankBalance:
    def __init__(self, denom: str, amount: Decimal) -> None:
        self.denom = denom
        self.amount = amount

    def update(self, amount: Decimal):
        self.amount = amount