"""Schema registry and compatibility checking.

A real Confluent Schema Registry is overkill for a single-node project and
would add a service whose failure modes we are not studying. Instead the
registry is an explicit, in-process map from ``(event_type, schema_version)``
to a pydantic model, plus a compatibility routine that answers the only
question the consumer actually asks: *can I read this payload?*
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from pydantic import ValidationError

from datatrust.contracts.base import EventEnvelope, EventPayload, EventType
from datatrust.contracts.events import (
    CustomerUpdatedV1,
    InventoryUpdatedV1,
    OrderPlacedV1,
    OrderPlacedV2,
    PaymentCapturedV1,
    RefundIssuedV1,
)
from datatrust.errors import ContractViolation, SchemaIncompatibility


class Compatibility(StrEnum):
    """How a candidate version relates to the registered latest version."""

    BACKWARD = "backward"
    """New schema can read data written with the old schema."""

    FORWARD = "forward"
    """Old schema can read data written with the new schema."""

    FULL = "full"
    """Both directions hold."""

    BREAKING = "breaking"
    """Neither direction holds. The consumer must reject or quarantine."""

    UNKNOWN = "unknown"
    """The version is not registered at all."""


@dataclass(frozen=True, slots=True)
class FieldSpec:
    """Flattened view of one field, used for structural comparison."""

    name: str
    required: bool
    type_repr: str


@dataclass(frozen=True, slots=True)
class ValidationFailure:
    """Structured evidence for a contract violation.

    Carried into quarantine verbatim. Every failed check in DataTrust produces
    a record of this shape so that incident evidence is queryable rather than
    a prose string in a log.
    """

    field_path: str
    message: str
    error_type: str
    observed: Any = None


@dataclass
class ContractRegistry:
    """Maps event type and version to the model that reads it."""

    _models: dict[tuple[EventType, int], type[EventPayload]] = field(default_factory=dict)

    def register(self, model: type[EventPayload]) -> None:
        """Register a payload model under its declared type and version."""
        event_type = _class_literal(model, "event_type")
        version = _class_literal(model, "schema_version")
        key = (EventType(event_type), int(version))
        if key in self._models and self._models[key] is not model:
            msg = f"{key} already registered to {self._models[key].__name__}"
            raise ValueError(msg)
        self._models[key] = model

    def register_all(self, models: Iterable[type[EventPayload]]) -> None:
        for model in models:
            self.register(model)

    def model_for(self, event_type: EventType, version: int) -> type[EventPayload]:
        """Return the model for a version, or raise :class:`SchemaIncompatibility`."""
        try:
            return self._models[(EventType(event_type), int(version))]
        except KeyError as exc:
            known = sorted(v for (t, v) in self._models if t == EventType(event_type))
            msg = (
                f"no schema registered for {EventType(event_type).value} v{version}; "
                f"known versions: {known or 'none'}"
            )
            raise SchemaIncompatibility(msg) from exc

    def versions_for(self, event_type: EventType) -> list[int]:
        return sorted(v for (t, v) in self._models if t == EventType(event_type))

    def latest_version(self, event_type: EventType) -> int:
        versions = self.versions_for(event_type)
        if not versions:
            msg = f"no schema registered for {EventType(event_type).value}"
            raise SchemaIncompatibility(msg)
        return versions[-1]

    # -- validation ---------------------------------------------------------

    def validate(self, envelope: EventEnvelope) -> EventPayload:
        """Parse an envelope's payload against its declared contract.

        Raises :class:`SchemaIncompatibility` when the version is unknown and
        :class:`ContractViolation` when the payload fails the known contract.
        Both are permanent errors: retrying an unreadable event does not make
        it readable, so the caller should quarantine rather than retry.
        """
        model = self.model_for(envelope.event_type, envelope.schema_version)
        try:
            return model.model_validate(envelope.payload)
        except ValidationError as exc:
            failures = failures_from_validation_error(exc)
            first = failures[0] if failures else None
            msg = (
                f"{envelope.event_type.value} v{envelope.schema_version} "
                f"failed contract: {first.message if first else 'unknown'}"
            )
            violation = ContractViolation(msg, field=first.field_path if first else None)
            violation.failures = failures  # type: ignore[attr-defined]
            raise violation from exc

    def collect_failures(self, envelope: EventEnvelope) -> list[ValidationFailure]:
        """Validate and return every failure instead of raising on the first.

        Used by the quality engine and the Failure Lab, where showing an
        operator all four broken fields is more useful than showing one.
        """
        try:
            model = self.model_for(envelope.event_type, envelope.schema_version)
        except SchemaIncompatibility as exc:
            return [
                ValidationFailure(
                    field_path="schema_version",
                    message=str(exc),
                    error_type="schema_incompatibility",
                    observed=envelope.schema_version,
                )
            ]
        try:
            model.model_validate(envelope.payload)
        except ValidationError as exc:
            return failures_from_validation_error(exc)
        return []

    # -- compatibility ------------------------------------------------------

    def compatibility(self, event_type: EventType, version: int) -> Compatibility:
        """Classify a version against the latest registered one.

        Structural comparison only: a field that is required in the reader but
        absent-and-defaultless in the writer breaks reading, and vice versa.
        This is intentionally coarser than a full Avro resolution — it is the
        subset the ingestion path depends on, and it is testable.
        """
        try:
            candidate = self.model_for(event_type, version)
        except SchemaIncompatibility:
            return Compatibility.UNKNOWN

        latest = self.model_for(event_type, self.latest_version(event_type))
        if candidate is latest:
            return Compatibility.FULL

        backward = _can_read(reader=latest, writer=candidate)
        forward = _can_read(reader=candidate, writer=latest)
        if backward and forward:
            return Compatibility.FULL
        if backward:
            return Compatibility.BACKWARD
        if forward:
            return Compatibility.FORWARD
        return Compatibility.BREAKING


#: Fields that identify *which* contract this is rather than carrying data.
#: They necessarily differ between versions, so including them in the
#: structural comparison would make every pair of versions look breaking.
VERSION_MARKER_FIELDS = frozenset({"event_type", "schema_version"})


def field_specs(
    model: type[EventPayload], *, include_markers: bool = False
) -> dict[str, FieldSpec]:
    """Flatten a model's fields for structural comparison."""
    specs: dict[str, FieldSpec] = {}
    for name, info in model.model_fields.items():
        if not include_markers and name in VERSION_MARKER_FIELDS:
            continue
        specs[name] = FieldSpec(
            name=name,
            required=info.is_required(),
            type_repr=str(info.annotation),
        )
    return specs


def _can_read(*, reader: type[EventPayload], writer: type[EventPayload]) -> bool:
    """Can ``reader`` deserialize a payload produced by ``writer``?

    Two ways it cannot: the reader requires a field the writer never emits, or
    the writer emits a field the reader forbids (every payload model sets
    ``extra="forbid"``, so unknown fields are a hard error by design).
    """
    reader_fields = field_specs(reader)
    writer_fields = field_specs(writer)

    for name, spec in reader_fields.items():
        if spec.required and name not in writer_fields:
            return False

    forbids_extra = reader.model_config.get("extra") == "forbid"
    if forbids_extra and set(writer_fields) - set(reader_fields):
        return False

    for name in set(reader_fields) & set(writer_fields):
        if reader_fields[name].type_repr != writer_fields[name].type_repr:
            return False

    return True


def failures_from_validation_error(exc: ValidationError) -> list[ValidationFailure]:
    """Convert a pydantic error into queryable evidence rows."""
    failures: list[ValidationFailure] = []
    for error in exc.errors():
        path = ".".join(str(part) for part in error["loc"]) or "<root>"
        failures.append(
            ValidationFailure(
                field_path=path,
                message=error["msg"],
                error_type=error["type"],
                observed=_safe_observed(error.get("input")),
            )
        )
    return failures


def _safe_observed(value: Any) -> Any:
    """Keep evidence small and JSON-serializable.

    Quarantine rows are written to Postgres as JSONB; a multi-megabyte payload
    echoed back as "observed" would make the incident table the biggest thing
    in the database.
    """
    if value is None or isinstance(value, bool | int | float):
        return value
    text = str(value)
    return text if len(text) <= 200 else text[:197] + "..."


def _class_literal(model: type[EventPayload], attr: str) -> Any:
    """Read a ``Literal`` field's single allowed value off the class.

    Payload models pin ``event_type`` and ``schema_version`` to literals, so
    the registry can index them without constructing an instance (which would
    require inventing valid values for every other field).
    """
    info = model.model_fields[attr]
    default = info.get_default(call_default_factory=False)
    if default is not None and default is not Ellipsis:
        return default
    args = getattr(info.annotation, "__args__", ())
    if len(args) == 1:
        return args[0]
    msg = f"{model.__name__}.{attr} must be a single-valued Literal with a default"
    raise TypeError(msg)


def _build_default_registry() -> ContractRegistry:
    registry = ContractRegistry()
    registry.register_all(
        [
            OrderPlacedV1,
            OrderPlacedV2,
            PaymentCapturedV1,
            RefundIssuedV1,
            CustomerUpdatedV1,
            InventoryUpdatedV1,
        ]
    )
    return registry


default_registry = _build_default_registry()
