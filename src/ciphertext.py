from dataclasses import dataclass

from parameter import FHEParameter


@dataclass(frozen=True)
class Ciphertext:
    """
    Ciphertext object: used for operation-level estimation.
    Note: level means CURRENT remaining level, i.e., the number of times it can still be dropped.
    fresh ciphertext has level = params.L, and each mul/add may drop the level by 1 or more.
    """

    level: int = 23
    ct_components: int = 2
    q_limbs_override: int | None = None

    def q_limbs(self, params: FHEParameter) -> int:
        if self.q_limbs_override is not None:
            return max(1, int(self.q_limbs_override))
        return params.q_limbs_at_level(self.level)

    def limb_count(self, params: FHEParameter) -> int:
        return params.ciphertext_limbs(
            q_limbs=self.q_limbs(params),
            ct_components=self.ct_components,
        )

    def byte_size(self, params: FHEParameter) -> int:
        return params.ciphertext_bytes(
            q_limbs=self.q_limbs(params),
            ct_components=self.ct_components,
        )

    def drop_level(self, delta: int = 1) -> "Ciphertext":
        """Returns a new ciphertext object with the level dropped."""
        new_level = max(0, self.level - max(0, int(delta)))
        return Ciphertext(
            level=new_level,
            ct_components=self.ct_components,
            q_limbs_override=self.q_limbs_override,
        )


@dataclass(frozen=True)
class Plaintext:
    """Plaintext object: used for operation-level estimation."""

    level: int = 0
    pt_components: int = 1
    q_limbs_override: int | None = None

    def q_limbs(self, params: FHEParameter) -> int:
        """Returns the number of q limbs corresponding to the current plaintext."""
        if self.q_limbs_override is not None:   
            return max(1, int(self.q_limbs_override))
        return params.q_limbs_at_level(self.level)

    def limb_count(self, params: FHEParameter) -> int:
        """Returns the total number of limbs for the current plaintext."""
        return params.ciphertext_limbs(
            q_limbs=self.q_limbs(params),
            ct_components=self.pt_components,
        )

    def byte_size(self, params: FHEParameter) -> int:
        """Returns the total byte size for the current plaintext."""
        return params.ciphertext_bytes(
            q_limbs=self.q_limbs(params),
            ct_components=self.pt_components,
        )
