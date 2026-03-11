"""
Primitive Level for FHE
NTT: Number Theoretic Transform
Basis Conversion: QP -> Q, QP -> P, etc.
Add: Modular addition of polynomials
Mul: Modular multiplication of polynomials
Automorphism: Galois automorphism for polynomial transformation
"""

import math

from parameter import FHEParameter

class Primitive:
    """FHE Primitive latency estimation model."""

    def __init__(self, params: FHEParameter):
        self.params = params
        self.clock_mhz = params.clock_mhz
        self.waves_per_poly = params.vectors_per_limb
        self.coeffs_per_wave = params.Lanes_Num


        # Simple command cost model: phase_cycles = startup + phase_waves * per_wave + drain
        # (startup, per_wave, drain) are the startup cycles, per wave cycles, and drain cycles for each command type.
        self.command_cost = {
            "lane_addsub": (1, 1, 0),
            "lane_mul": (13, 1, 0),
            "stream_mul": (0, 1, 0),
            "row_ntt_fwd": (0, 1, 0),
            "row_ntt_inv": (0, 1, 0),
            "spu_stream": (1, 2, 1),
            "bconv": (13, 1, 2),
        }

        # Pre-compute primitive latencies in us based on the command cost model
        self.ntt_latency = self.ntt_latency_estimate()
        self.intt_latency = self.intt_latency_estimate()
        self.one_limb_BConv_latency = self.one_limb_BConv_latency_estimate()
        self.add_latency = self.add_latency_estimate()
        self.mul_latency = self.mul_latency_estimate()
        self.automorphism_latency = self.automorphism_latency_estimate()

    def _phase_cycles(self, cmd: str, wave_passes: float) -> int:
        """Calculate the total cycles for a given command and number of wave passes."""
        startup, per_wave, drain = self.command_cost[cmd]
        phase_waves = math.ceil(self.waves_per_poly * float(wave_passes) - 1e-12)
        return startup + phase_waves * per_wave + drain

    def _primitive_latency_us(self, phases: list[tuple[str, float]], barrier_cycles: int = 0) -> float:
        """Calculate the total latency (in us) for a primitive given its phases and optional barrier cycles."""
        total_cycles = sum(self._phase_cycles(cmd, wave_passes) for cmd, wave_passes in phases)
        total_cycles += max(0, int(barrier_cycles))
        return total_cycles / self.clock_mhz
    
    def ntt_latency_estimate(self):
        """Four Steps NTT: psi pre-processing -> transpose -> row_ntt -> twiddle -> transpose -> row_ntt."""
        phases = [
            ("stream_mul", 1),
            ("spu_stream", 1),
            ("row_ntt_fwd", 8),
            ("stream_mul", 1),
            ("spu_stream", 1),
            ("row_ntt_fwd", 8),
        ]
        return self._primitive_latency_us(phases)

    def intt_latency_estimate(self):
        """Four Steps INTT: transpose -> row_intt -> twiddle -> transpose -> row_intt -> psi post-processing."""
        phases = [
            ("spu_stream", 1),
            ("row_ntt_inv", 8),
            ("stream_mul", 1),
            ("spu_stream", 1),
            ("row_ntt_inv", 8),
            ("stream_mul", 1),
        ]
        return self._primitive_latency_us(phases)
    
    def add_latency_estimate(self):
        """ModAdd/ModSub latency estimation: 1 lane_addsub phase."""
        return self._primitive_latency_us([("lane_addsub", 1)])

    def mul_latency_estimate(self):
        """ModMult latency estimation: 1 lane_mul phase."""
        return self._primitive_latency_us([("lane_mul", 1)])

    def automorphism_latency_estimate(self):
        """Automorphism latency estimation: 1 spu_stream phase."""
        return self._primitive_latency_us([("spu_stream", 1)])

    def one_limb_BConv_latency_estimate(self, lin: int | None = None, lout: int | None = None):
        """
        Base case of Basis Conversion latency estimation: 1 bconv phase.
        
        BConv is matrix multiplication operation:
        - Input: alpha limbs
        - Output: 1 limb
        Resource allocation strategy (based on 256 lanes):
        - Every alpha lanes allocated for one output limb
        - Can process (256 / alpha) coefficients of BConv in parallel
        - Requires (lin - 1) addition iterations for accumulation
        """
      
        lin = max(1, int(self.params.alpha if lin is None else lin))
        lout = 1
        
        # Parallelism for each iteration: how many coefficients can be processed in parallel given the number of lanes and output limbs
        parallel_coeffs = self.params.Lanes_Num // lin  # 256 / alpha
        
        # Total iterations needed to process all coefficients of the BConv given the parallelism
        iterations = math.ceil(self.coeffs_per_wave / parallel_coeffs)
         
        # Each iteration consists of one bconv phase, and we add a small barrier to account for any overhead between iterations.
        phases = [
            ("bconv", iterations),  
        ]
        return self._primitive_latency_us(phases, barrier_cycles=16)
    
    @property
    def bconv_latency(self) -> float:
        """General BConv latency estimation: 1 bconv phase with alpha input limbs."""
        return self.one_limb_BConv_latency

    @property
    def auto_latency(self) -> float:
        """Automorphism latency estimation: 1 spu_stream phase."""
        return self.automorphism_latency
    
if __name__ == "__main__":
    params = FHEParameter()
    primitive = Primitive(params)
    print("="*60)
    print("FHE Primitive operations initialized with parameters:")
    print("="*60)

    print("Estimated NTT latency:", primitive.ntt_latency)
    print("Estimated INTT latency:", primitive.intt_latency)
    print("Estimated One limb Basis Conversion latency:", primitive.one_limb_BConv_latency)
    print("Estimated Addition latency:", primitive.add_latency)
    print("Estimated Multiplication latency:", primitive.mul_latency)
    print("Estimated Automorphism latency:", primitive.automorphism_latency)
