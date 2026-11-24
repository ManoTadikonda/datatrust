"""Exception hierarchy.

Errors are typed so that callers can distinguish *transient* failures (worth a
bounded retry) from *permanent* ones (worth quarantine or manual review).
Nothing in DataTrust catches bare ``Exception`` and continues.
"""

from __future__ import annotations


class DataTrustError(Exception):
    """Base class for every DataTrust error."""


class TransientError(DataTrustError):
    """A failure that a bounded retry may resolve (broker blip, lock timeout)."""


class PermanentError(DataTrustError):
    """A failure that retrying cannot fix (bad payload, incompatible schema)."""


class ContractViolation(PermanentError):
    """An event does not satisfy its declared data contract."""

    def __init__(self, message: str, *, field: str | None = None) -> None:
        super().__init__(message)
        self.field = field


class SchemaIncompatibility(PermanentError):
    """A schema version cannot be read by the current consumer."""


class IllegalStateTransition(DataTrustError):
    """An incident was asked to move between two states that are not adjacent."""

    def __init__(self, current: str, requested: str) -> None:
        super().__init__(f"illegal incident transition: {current} -> {requested}")
        self.current = current
        self.requested = requested


class RecoveryNotEligible(DataTrustError):
    """Recovery was requested for an incident that policy forbids repairing."""


class VerificationFailed(DataTrustError):
    """A recovery completed but its independent verification did not pass."""
