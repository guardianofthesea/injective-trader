from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from threading import Event
from typing import Dict, Optional

import requests
import valkey
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from injective_trader.utils.logger import ThreadLogger


@dataclass(frozen=True, slots=True)
class Config:
    # Core
    name: str
    log_path: str
    asset: str
    decimals: int = 18
    interval: float = 0.25  # seconds

    # Stork API
    api_base_url: str = "https://rest.jp.stork-oracle.network/v1/prices/latest"
    auth_token: str = field(
        default_factory=lambda: os.getenv("STORK_AUTH", "")
    )
    http_retries: int = 3
    http_backoff: float = 0.2  # base back‑off in seconds
    http_timeout: float = 5.0  # request timeout

    # Valkey
    valkey_host: str = field(default_factory=lambda: os.getenv("VALKEY_HOST", "localhost"))
    valkey_port: int = field(
        default_factory=lambda: int(os.getenv("VALKEY_PORT", "6379"))
    )
    valkey_db: int = field(default_factory=lambda: int(os.getenv("VALKEY_DB", "0")))
    valkey_channel: str = field(default_factory=lambda: os.getenv("VALKEY_CHANNEL", ""))

    # Misc
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))

    # Circuit breaker
    max_consecutive_errors: int = 8


def build_http_session(cfg: Config) -> requests.Session:
    """Return a requests Session with retry + timeout defaults."""
    retry_strategy = Retry(
        total=cfg.http_retries,
        backoff_factor=cfg.http_backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    sess = requests.Session()
    sess.mount("https://", adapter)
    sess.headers.update(
        {
            "Authorization": cfg.auth_token,
            "Content-Type": "application/json",
        }
    )
    return sess


class PriceFetcher:
    """Lightweight wrapper around Stork price API."""

    def __init__(self, cfg: Config, session: Optional[requests.Session] = None):
        self.cfg = cfg
        self.session = session or build_http_session(cfg)

    def fetch(self) -> Dict:
        """Return a structured price payload."""
        url = f"{self.cfg.api_base_url}?assets={self.cfg.asset}"
        resp = self.session.get(url, timeout=self.cfg.http_timeout)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Unexpected HTTP {resp.status_code}: {resp.text[:120]}"
            )

        payload = resp.json().get("data", {})
        if self.cfg.asset not in payload:
            raise KeyError(f"{self.cfg.asset} key missing in API response")

        asset_data = payload[self.cfg.asset]
        price_raw = float(asset_data["price"])
        price_adj = price_raw / (10 ** self.cfg.decimals)

        return {
            # WARNING: This is only a demo, the external info format should be defined by user
            "market_id": "0xe5bfc48fc29146d756c9dac69f096d56cc4fc5ae75c98c1ad045c3356d14eb82",
            # "strategy_name": "MMStrategy",
            "timestamp": asset_data.get("timestamp", int(time.time())),
            "price": price_adj,
            "is_stork": True,
        }

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:  # pragma: no cover
            logging.getLogger(__name__).exception("failed to close session")


class ValkeyPublisher:
    """Thread‑safe publisher with lazy connection initialisation."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._client: Optional[valkey.Valkey] = None
        self._channel = (
            cfg.valkey_channel or f"{cfg.asset.lower()}_price_updates"
        )

    @property
    def client(self) -> valkey.Valkey:
        if self._client is None:
            self._client = valkey.Valkey(
                host=self.cfg.valkey_host,
                port=self.cfg.valkey_port,
                db=self.cfg.valkey_db,
                socket_keepalive=True,
            )
        return self._client

    def publish(self, message: Dict) -> None:
        self.client.publish(self._channel, json.dumps(message))

    def close(self) -> None:
        if self._client and hasattr(self._client, "close"):
            try:
                self._client.close()
            except Exception:  # pragma: no cover
                logging.getLogger(__name__).exception("failed to close valkey")


class Runner:
    """Encapsulates the main polling / publish conseconsole_level_index=1,file_vefile_level=loop."""

    def __init__(self, *, cfg: Config, logger):
        self.cfg = cfg
        self.stop_event = Event()
        self.fetcher = PriceFetcher(cfg)
        self.publisher = ValkeyPublisher(cfg)
        self.logger = logger

    # ----- graceful shutdown -------------------------------------------------
    def _install_signal_handlers(self) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._signal_handler)

    def _signal_handler(self, *_):
        if not self.stop_event.is_set():
            self.logger.info("Shutdown signal received…")
            self.stop_event.set()

    # ----- run loop ----------------------------------------------------------
    def run(self) -> None:
        self._install_signal_handlers()
        errors = 0

        self.logger.info(
            "Starting poll @ %.2fs for %s → Valkey channel '%s'",
            self.cfg.interval,
            self.cfg.asset,
            self.publisher._channel,
        )
        while not self.stop_event.is_set():
            start = time.perf_counter()
            try:
                data = self.fetcher.fetch()
                self.publisher.publish(data)
                self.logger.debug("Published: %s", data)
                errors = 0
            except Exception as exc:  # noqa: BLE001
                errors += 1
                self.logger.warning("Failure %d: %s", errors, exc, exc_info=True)
                if errors >= self.cfg.max_consecutive_errors:
                    self.logger.error(
                        "Exceeded %d consecutive errors – aborting",
                        self.cfg.max_consecutive_errors,
                    )
                    break

            # wait the remaining interval or until stop
            elapsed = time.perf_counter() - start
            delay = max(0.0, self.cfg.interval - elapsed)
            self.stop_event.wait(delay)

        self.logger.info("Runner exiting…")
        self.cleanup()

    def cleanup(self) -> None:
        self.publisher.close()
        self.fetcher.close()


def parse_args() -> Config:
    p = argparse.ArgumentParser(description="Robust price‑fetcher service")

    # existing options
    p.add_argument("-a", "--asset", required=True, help="Asset symbol, e.g. ETH")
    p.add_argument("-d", "--decimals", type=int, default=18)
    p.add_argument("-i", "--interval", type=float, default=0.25)
    p.add_argument("--valkey-host", default=os.getenv("VALKEY_HOST", "localhost"))
    p.add_argument("--valkey-port", type=int, default=int(os.getenv("VALKEY_PORT", 6379)))
    p.add_argument("--valkey-db", type=int, default=int(os.getenv("VALKEY_DB", 0)))
    p.add_argument("--valkey-channel", default=os.getenv("VALKEY_CHANNEL", ""))

    # new options
    p.add_argument(
        "-n", "--name",
        help="Human‑readable fetcher name used in logs (default: <asset>_price_fetcher)"
    )
    p.add_argument(
        "--log-path",
        help="Full path or filename for the log file "
             "(default: ./<name>.log, created lazily in main())"
    )

    p.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    args = p.parse_args()

    # fallbacks computed *after* parsing so we know the asset
    name = args.name or f"{args.asset.lower()}_price_fetcher"
    log_path = args.log_path or f"./{name}.log"

    return Config(
        asset=args.asset,
        decimals=args.decimals,
        interval=args.interval,
        valkey_host=args.valkey_host,
        valkey_port=args.valkey_port,
        valkey_db=args.valkey_db,
        valkey_channel=args.valkey_channel,
        log_level=args.log_level,
        name=name,
        log_path=log_path,
    )


def main() -> None:
    cfg = parse_args()

    thread_logger = ThreadLogger(name=cfg.name, log_file=cfg.log_path, console_level="INFO", file_level="DEBUG")
    logger = thread_logger.get_logger()

    if not cfg.auth_token:
        logger.error(
            "STORK_AUTH environment variable is missing. "
            "Export it or pass via --auth-token flag."
        )
        thread_logger.shutdown()
    else:
        Runner(cfg=cfg, logger=logger).run()

if __name__ == "__main__":  # pragma: no cover
    main()
