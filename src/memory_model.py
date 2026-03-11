"""
Memory model estimation for IO and spill costs.
"""
from dataclasses import dataclass
from parameter import FHEParameter

@dataclass(frozen=True)
class MemoryEstimate:
    """Memory estimate for an operation, including IO and spill costs."""
    read_us: float = 0.0    # Read latency in us
    write_us: float = 0.0   # Write latency in us
    io_us: float = 0.0      # IO latency in us
    spill_us: float = 0.0   # Spill latency in us
    read_bytes: int = 0     # Number of bytes read from HBM
    write_bytes: int = 0    # Number of bytes written to HBM
    spill_bytes: int = 0    # Number of bytes spilled to HBM
    read_limbs: int = 0     # Number of limbs read
    write_limbs: int = 0    # Number of limbs written
    spill_limbs: int = 0    # Number of limbs spilled
  

def estimate_io_and_spill_us(
    params: FHEParameter,
    read_limbs: int = 0,
    write_limbs: int = 0,
    input_onchip: bool = False,
    output_onchip: bool = False,
    working_set_limbs: int  | None = None,
) -> MemoryEstimate:
    """
    Estimate operation HBM io latency
    - input_onchip: Whether the input is on-chip (if False, it needs to be read from HBM)
    - output_onchip: Whether the output is on-chip (if False, it needs to be written to HBM)
    - if working set overflows on-chip capacity, we also add spill cost to the estimate
    """
    
    read_limbs = max(0, int(read_limbs))
    write_limbs = max(0, int(write_limbs))
    
    actual_read_limbs = 0 if input_onchip else read_limbs
    actual_write_limbs = 0 if output_onchip else write_limbs
    
    read_bytes = actual_read_limbs * params.limb_bytes
    write_bytes = actual_write_limbs * params.limb_bytes
    read_us = params.hbm_read_time_us(read_bytes) 
    write_us = params.hbm_write_time_us(write_bytes)
    io_us = params.hbm_io_time_us(read_bytes=read_bytes, write_bytes=write_bytes)
    
    spill_us = 0.0
    spill_bytes = 0.0
    spill_limbs = 0
    if working_set_limbs is not None:
        working_set_limbs = max(0, int(working_set_limbs))
        overflow_limbs = max(0, working_set_limbs - params.onchip_capacity_limbs)
        if overflow_limbs > 0:
            spill_limbs = overflow_limbs
            spill_bytes = spill_limbs * params.limb_bytes
            spill_us = params.hbm_io_time_us(read_bytes=spill_bytes, write_bytes=spill_bytes)

    return MemoryEstimate(
        read_us=read_us,
        write_us=write_us,
        io_us=io_us,
        spill_us=spill_us,
        read_bytes=read_bytes,
        write_bytes=write_bytes,
        spill_bytes=spill_bytes,
        read_limbs=actual_read_limbs,
        write_limbs=actual_write_limbs,
        spill_limbs=spill_limbs,
    )
    
def estimate_materialized_working_set_us(
    params: FHEParameter,
    working_set_limbs: int,
) -> float:
    """Estimate the latency of materializing a working set in HBM."""
    working_set_limbs = max(0, int(working_set_limbs))
    overflow = max(0, working_set_limbs - params.onchip_limb_capacity)
    if overflow <= 0:
        return 0.0
    
    spill_bytes = overflow * params.limb_bytes
    return params.hbm_io_time_us(read_bytes=spill_bytes, write_bytes=spill_bytes)
    