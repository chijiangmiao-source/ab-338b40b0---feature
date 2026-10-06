"""Runtime configuration, sourced from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


def _int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


@dataclass(frozen=True)
class Settings:
    """Tunables for the monitoring engine.

    Levels are derived per sealed window:
      CRITICAL  if total_dose >= critical_total or peak_dose >= critical_peak
      ELEVATED  if total_dose >= elevated_total or peak_dose >= elevated_peak
      NORMAL    otherwise
    """

    database_path: str = "radiation.db"
    window_seconds: int = 300
    allowed_lateness_seconds: int = 60
    elevated_total: float = 100.0
    elevated_peak: float = 40.0
    critical_total: float = 250.0
    critical_peak: float = 80.0
    probe_alpha: str = "alpha"
    probe_beta: str = "beta"

    @property
    def window_ms(self) -> int:
        return self.window_seconds * 1000

    @property
    def lateness_ms(self) -> int:
        return self.allowed_lateness_seconds * 1000

    @property
    def probes(self) -> tuple[str, str]:
        return (self.probe_alpha, self.probe_beta)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Settings":
        env = os.environ if env is None else env
        return cls(
            database_path=env.get("DATABASE_PATH", "radiation.db"),
            window_seconds=_int(env, "WINDOW_SECONDS", 300),
            allowed_lateness_seconds=_int(env, "ALLOWED_LATENESS_SECONDS", 60),
            elevated_total=_float(env, "ELEVATED_TOTAL", 100.0),
            elevated_peak=_float(env, "ELEVATED_PEAK", 40.0),
            critical_total=_float(env, "CRITICAL_TOTAL", 250.0),
            critical_peak=_float(env, "CRITICAL_PEAK", 80.0),
            probe_alpha=env.get("PROBE_ALPHA", "alpha"),
            probe_beta=env.get("PROBE_BETA", "beta"),
        )
