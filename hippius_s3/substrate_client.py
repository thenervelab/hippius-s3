"""Minimal Substrate client for account verification and credit queries."""

from typing import Optional


class SubstrateClient:
    """
    Minimal Substrate client for Hippius S3.

    Only includes the functionality needed for account verification
    and credit queries. Extracted from hippius-sdk to avoid external dependency.
    """

    def __init__(
        self,
        url: Optional[str] = None,
        password: Optional[str] = None,
        account_name: Optional[str] = None,
        seed_phrase: Optional[str] = None,
    ):
        """
        Initialize the Substrate client.

        Args:
            url: WebSocket URL of the Hippius substrate node
            password: Optional password (not used in minimal version)
            account_name: Optional account name (not used in minimal version)
            seed_phrase: Optional unencrypted seed phrase to use directly
        """
        self.url = url or "wss://rpc.hippius.network"
        self._substrate = None
        self._keypair = None
        self._account_name = account_name
        self._account_address = None
        self._read_only = False
        self._seed_phrase_password = password
        self._seed_phrase = seed_phrase

    def connect(self, seed_phrase: Optional[str] = None) -> None:
        """
        Connect to the Substrate node.

        Initializes the connection to the Substrate node and creates a keypair from the seed phrase.

        Args:
            seed_phrase: Optional seed phrase for the connection
        """
        from substrateinterface import SubstrateInterface

        self._substrate = SubstrateInterface(
            url=self.url,
            ss58_format=42,
            type_registry_preset="substrate-node-template",
        )

        if self._ensure_keypair(seed_phrase):
            assert self._keypair is not None
            self._read_only = False
        elif self._account_address:
            self._read_only = True
        else:
            self._read_only = True

    def _ensure_keypair(self, seed_phrase: Optional[str] = None) -> bool:
        """
        Ensure we have a keypair for signing transactions.
        Will use the provided seed_phrase if given, otherwise get it from stored seed phrase.

        Args:
            seed_phrase: Optional seed phrase to use for creating keypair

        Returns:
            bool: True if keypair is available, False if it couldn't be created
        """
        from substrateinterface.keypair import Keypair

        if self._keypair and not seed_phrase:
            return True

        if seed_phrase:
            self._keypair = Keypair.create_from_mnemonic(seed_phrase)
            assert self._keypair is not None
            self._account_address = self._keypair.ss58_address
            self._read_only = False
            return True

        if self._seed_phrase:
            self._keypair = Keypair.create_from_mnemonic(self._seed_phrase)
            assert self._keypair is not None
            self._account_address = self._keypair.ss58_address
            self._read_only = False
            return True

        return False

    def query_sub_account(self, account_id: str, seed_phrase: str) -> str | None:
        """
        Query if an account is a sub-account.

        Args:
            account_id: Account ID to query
            seed_phrase: Seed phrase for authentication

        Returns:
            str | None: Sub-account data if it exists, None if it's a main account
        """
        if not self._substrate:
            self.connect(seed_phrase)

        assert self._substrate is not None
        result = self._substrate.query(module="SubAccount", storage_function="SubAccount", params=[account_id])

        if result and hasattr(result, "value") and result.value:
            return str(result.value)
        return None

    def is_main_account(self, account_id: str, seed_phrase: str) -> bool:
        """
        Check if an account is a main account (not a sub-account).

        Args:
            account_id: Account ID to check
            seed_phrase: Seed phrase for authentication

        Returns:
            bool: True if main account, False if sub-account
        """
        sub_account = self.query_sub_account(account_id, seed_phrase=seed_phrase)
        return sub_account is None

    async def get_free_credits(self, account_address: str, seed_phrase: Optional[str] = None) -> float:
        """
        Query free credits for an account from the Credits pallet.

        Args:
            account_address: The SS58 account address to query credits for
            seed_phrase: Optional seed phrase (unused, kept for API compat)

        Returns:
            float: The free credits balance (raw u128 divided by 10^18)
        """
        if not self._substrate:
            self.connect(seed_phrase)

        assert self._substrate is not None
        result = self._substrate.query(
            module="Credits",
            storage_function="FreeCredits",
            params=[account_address],
        )

        if result and hasattr(result, "value") and result.value:
            return int(result.value) / 10**18

        return 0.0
