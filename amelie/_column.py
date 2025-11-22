"""
Module defining the Column class representing a database result set column.

NOTE: NOT IMPLEMENTED AS NO WAY TO GET ROW METADATA FROM AMELIEDB SERVER
"""


class Column:
    """Represents a single column in a database result set."""

    def __init__(
        self,
        name,
        type_code,
        display_size=None,
        internal_size=None,
        precision=None,
        scale=None,
        null_ok=None,
    ):
        self.name = name
        self.type_code = type_code
        self.display_size = display_size
        self.internal_size = internal_size
        self.precision = precision
        self.scale = scale
        self.null_ok = null_ok

    def __repr__(self):
        return (
            f"Column(name={self.name}, type_code={self.type_code}, "
            f"display_size={self.display_size}, internal_size={self.internal_size}, "
            f"precision={self.precision}, scale={self.scale}, null_ok={self.null_ok})"
        )

    def to_dict(self):
        """Convert the Column instance to a dictionary."""
        return {
            "name": self.name,
            "type_code": self.type_code,
            "display_size": self.display_size,
            "internal_size": self.internal_size,
            "precision": self.precision,
            "scale": self.scale,
            "null_ok": self.null_ok,
        }

    @classmethod
    def from_dict(cls, data):
        """Create a Column instance from a dictionary."""
        return cls(
            name=data.get("name"),
            type_code=data.get("type_code"),
            display_size=data.get("display_size"),
            internal_size=data.get("internal_size"),
            precision=data.get("precision"),
            scale=data.get("scale"),
            null_ok=data.get("null_ok"),
        )
