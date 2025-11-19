from __future__ import annotations
import yaml
import logging
from pathlib import Path
from typing import Dict, Union, Any, Optional, List,Type, Mapping
from pydantic import BaseModel, Field, model_validator, ValidationError

class ComponentConfig(BaseModel):
    """Configuration for a core framework component"""
    params: Dict[str, Any] = Field(default_factory=dict)

class StrategyConfig(BaseModel):
    """Configuration for a trading strategy"""
    Name: str
    Class: str
    MarketIds: List[str]
    AccountAddresses: List[str]
    params: Dict[str, Any] = Field(default_factory=dict)

class RetryComponentConfig(BaseModel):
    """Configuration for retry behavior of a component"""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 32.0
    jitter: bool = True
    timeout: float = 30.0
    error_threshold: int = 10
    error_window: int = 60

class BotConfig(BaseModel):
    """Root configuration for the trading bot"""
    Exchange: str
    ConsoleLevel: str = "INFO"
    FileLevel: str = "DEBUG"
    Components: Dict[str, Dict[str, Any]]
    Strategies: Dict[str, Dict[str, Any]]
    RetryConfig: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

    @model_validator(mode='before')
    @classmethod
    def handle_legacy_format(cls, data):
        """Handle legacy format conversion if needed"""
        if isinstance(data, dict):
            # Already in the expected format, just ensure all required sections exist
            if "Components" not in data:
                data["Components"] = {}
            if "Strategies" not in data:
                data["Strategies"] = {}
            if "RetryConfig" not in data:
                data["RetryConfig"] = {}

            # Default values if missing
            if "Exchange" not in data:
                data["Exchange"] = "Helix"
            if "LogLevel" not in data:
                data["LogLevel"] = "INFO"

        return data

    def get_component_config(self, component_name: str) -> Dict[str, Any]:
        """Get configuration for a specific component"""
        return self.Components.get(component_name, {})

    def get_strategy_config(self, strategy_name: str) -> Dict[str, Any]:
        """Get configuration for a specific strategy"""
        return self.Strategies.get(strategy_name, {})

    def get_retry_config(self, component_name: None|str = None) -> Dict[str, Any]:
        """Get retry configuration for a component or default"""
        if component_name and component_name in self.RetryConfig:
            return self.RetryConfig[component_name]

        return self.RetryConfig.get("DefaultRetry", {})




# ────────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ────────────────────────────────────────────────────────────────────────────────

_VALID_SUFFIXES = {".yaml", ".yml"}
REQUIRED_SECTIONS: dict[str, Type] = {"Components": dict, "Exchange":   str}

def _load_yaml(path: Path) -> Dict[str, Any]:
    """Read *and* parse YAML, normalising “empty file” to an empty dict."""
    try:
        with path.open("r", encoding="utf-8") as fp:
            data = yaml.safe_load(fp) or {}
    except yaml.YAMLError as err:
        raise ValueError(f"YAML syntax error in {path}: {err}") from err
    except OSError as err:
        raise OSError(f"Unable to read config file {path}: {err}") from err
    if not isinstance(data, dict):
        raise TypeError(f"Top‑level YAML must be a mapping, got {type(data).__name__}")
    return data


def _validate_sections(
    cfg: dict[str, Any],
    required: dict[str, Type],
) -> None:
    """
    Ensure every key in `required` exists in `cfg` *and* is of the declared type.

    Parameters
    ----------
    cfg : Mapping[str, Any]
        Raw dict produced by `yaml.safe_load`.
    required : Mapping[str, Type]
        Section‑name → expected‑Python‑type.
    """
    for section, expected in required.items():
        if section not in cfg:
            raise KeyError(f"Missing required top‑level key '{section}'")

        value = cfg[section]
        if not isinstance(value, expected):
            raise TypeError(
                f"Section '{section}' must be of type "
                f"{expected.__name__}, got {type(value).__name__}"
            )

# ────────────────────────────────────────────────────────────────────────────────
# Public API
# ────────────────────────────────────────────────────────────────────────────────

def read_config(path: str | Path, logger: logging.Logger) -> BotConfig:
    """
    Load a YAML configuration file and return a fully validated `BotConfig`.

    Parameters
    ----------
    path : str | Path
        Location of the YAML file (.yaml | .yml).
    logger : logging.Logger
        Logger instance for diagnostics.

    Raises
    ------
    (ValueError, FileNotFoundError, TypeError, KeyError, ValidationError)
        Forwarded exceptions give precise failure causes.
    """
    p = Path(path)

    # ── Fast pre‑flight checks ──────────────────────────────────────────────
    if p.suffix not in _VALID_SUFFIXES:
        raise ValueError("Config file must have a .yaml or .yml extension")
    if not p.is_file():
        raise FileNotFoundError(p)

    # ── I/O and structural validation ──────────────────────────────────────
    logger.debug(f"Loading configuration from {p}")
    raw_cfg = _load_yaml(p)
    _validate_sections(raw_cfg, REQUIRED_SECTIONS)

    # ── Domain‑specific preprocessing (may mutate or return a copy) ────────
    processed_cfg = _preprocess_component_configs(raw_cfg, logger) or raw_cfg

    # ── Schema validation & instantiation ──────────────────────────────────
    try:
        bot_cfg = BotConfig(**processed_cfg)
    except ValidationError as err:
        logger.error("Configuration file failed schema validation: %s", err)
        raise

    logger.info(f"Configuration loaded successfully: {p.name}")
    return bot_cfg


def _preprocess_component_configs(config_data: Dict, logger):
    """
    Preprocess component configurations
    - Synthesize AuthzPools for MessageBroadcaster
    - Distribute RetryConfig to components
    """
    # Ensure required sections exist
    if "Components" not in config_data:
        config_data["Components"] = {}
    if "Strategies" not in config_data:
        config_data["Strategies"] = {}

    # Extract auth pools from strategies
    authz_pools = _extract_authz_pools_from_strategies(config_data["Strategies"])

    # Add to MessageBroadcaster config
    if "MessageBroadcaster" not in config_data["Components"]:
        config_data["Components"]["MessageBroadcaster"] = {}

    config_data["Components"]["MessageBroadcaster"]["AuthzPools"] = authz_pools

    # Distribute RetryConfig to components
    if "RetryConfig" in config_data:
        retry_configs = config_data["RetryConfig"]

        # For each component, add its retry config
        for component_name, component_config in config_data["Components"].items():
            if component_name in retry_configs:
                component_config["RetryConfig"] = retry_configs[component_name]
            elif "DefaultRetry" in retry_configs:
                component_config["RetryConfig"] = retry_configs["DefaultRetry"]

def _extract_authz_pools_from_strategies(strategies_config: Dict) -> List[Dict]:
    """
    Extract authorization pool information from strategy configurations.
    """
    authz_pools = []
    seen_granters = set()

    for strategy_name, strategy_config in strategies_config.items():
        # Skip non-authz strategies
        if "Granter" not in strategy_config or "Grantees" not in strategy_config:
            continue

        granter = strategy_config["Granter"]
        grantees = strategy_config["Grantees"]

        # Skip if we've already processed this granter
        if granter in seen_granters:
            continue

        seen_granters.add(granter)

        # Add to pools
        authz_pools.append({
            "granter": granter,
            "grantees": grantees
        })

    return authz_pools


def create_default_config(logger, network: str = "mainnet"):
    """
    Create a default configuration object when no config file is provided.
    
    Args:
        logger: Logger instance
        network: Network to use ("mainnet" or "testnet")
        private_key: Optional private key for transactions
        
    Returns:
        BotConfig: Default configuration object
    """
    logger.info(f"Creating default configuration for network: {network}")
    
    # Create minimal configuration dictionary
    default_config = {
        "Exchange": "Helix",
        "LogLevel": "INFO",
        
        "Components": {
            "Initializer": {
                "Network": network,
                "MarketTickers": [
                    "INJ/USDT PERP"  # Default to a common market
                ]
            },
            "ChainListener": {
                "ReconnectionDelay": 5,
                "LargeGapThreshold": 50
            },
            "MessageBroadcaster": {
                "ErrorCodesJson": "config/error_codes.json",
                "GranteePool": {
                    "MaxPendingTxs": 5,
                    "ErrorThreshold": 3,
                    "BlockDuration": 300,
                    "RotationInterval": 1
                },
                "RefreshInterval": 300,
                "Batch": {
                    "MaxBatchSize": 15,
                    "MinBatchSize": 3,
                    "MaxGasLimit": 5000000,
                    "MaxBatchDelay": 0.5
                }
            }
        },
        
        "Strategies": {},  # Empty by default, users will add strategies programmatically
        
        "RetryConfig": {
            "DefaultRetry": {
                "max_attempts": 3,
                "base_delay": 1.0,
                "max_delay": 32.0,
                "jitter": True,
                "timeout": 30.0,
                "error_threshold": 10,
                "error_window": 60
            },
            "ChainListener": {
                "max_attempts": 5,
                "base_delay": 2.0,
                "max_delay": 45.0
            },
            "MessageBroadcaster": {
                "max_attempts": 3,
                "base_delay": 1.0,
                "max_delay": 30.0
            }
        }
    }
    
    # Create BotConfig object
    try:
        config = BotConfig(**default_config)
        logger.info("Default configuration created successfully")
        return config
    except Exception as e:
        logger.error(f"Failed to create default configuration: {e}")
        raise ValueError(f"Failed to create default configuration: {e}")