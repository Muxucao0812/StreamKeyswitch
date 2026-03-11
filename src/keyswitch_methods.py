"""
keyswitch 方法库（可扩展）。

你可以在这个文件中新增自己的 keyswitch 方法，
并通过 register_keyswitch_method 注册到方法表。
"""

from collections.abc import Callable

from ciphertext import Ciphertext
from parameter import FHEParameter
from primitive import Primitive

KeySwitchFn = Callable[..., float]

_KEYSWITCH_METHODS: dict[str, KeySwitchFn] = {}


def default_keyswitch_method(
    *,
    ct: Ciphertext,
    primitive: Primitive,
    params: FHEParameter,
    digit_mul: float = 1.0,
    digit_add: float = 1.0,
    bconv_count: float = 1.0,
    extra_us: float = 0.0,
) -> float:
    """默认 keyswitch 延迟模型。"""
    q = ct.q_limbs(params)
    comps = max(1, int(ct.ct_components))
    beta = params.beta
    q_scale = q / max(1, params.Q_limbs)

    digit_us = comps * q * beta * (
        float(digit_mul) * primitive.mul_latency
        + float(digit_add) * primitive.add_latency
    )
    bconv_us = comps * float(bconv_count) * primitive.basis_conversion_latency * q_scale
    return digit_us + bconv_us + max(0.0, float(extra_us))


def register_keyswitch_method(name: str, fn: KeySwitchFn) -> None:
    """注册一个 keyswitch 方法。"""
    key = str(name).strip().lower()
    if not key:
        raise ValueError("keyswitch method name cannot be empty")
    _KEYSWITCH_METHODS[key] = fn


def get_keyswitch_method(name: str) -> KeySwitchFn:
    """按名字获取 keyswitch 方法。"""
    key = str(name).strip().lower()
    if key not in _KEYSWITCH_METHODS:
        known = ", ".join(sorted(_KEYSWITCH_METHODS))
        raise KeyError(f"unknown keyswitch method '{name}', available: {known}")
    return _KEYSWITCH_METHODS[key]


def list_keyswitch_methods() -> list[str]:
    """列出已注册的方法名。"""
    return sorted(_KEYSWITCH_METHODS)


# 默认方法注册
register_keyswitch_method("default", default_keyswitch_method)
