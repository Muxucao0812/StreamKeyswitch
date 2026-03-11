import math
from dataclasses import dataclass


@dataclass(frozen=True)
class FHEParameter:
    """FHE parameters"""
    
    # ------------------------------
    # Parameters for the FHE scheme
    # ------------------------------
    N: int = 65536          # polynomial degree = 2^16
    logq_bits: int = 50     # residue modulus bit-width
    logp_bits: int = 50 
    
    L: int = 23             # number of limbs for modulus Q
    k: int = 9              # number of limbs for modulus k
    dnum: int = 3           # number of limbs per digital


    # ------------------------------
    # Parameters for the FPGA 
    # ------------------------------
    clock_mhz: float = 300.0       # FPGA frequency (MHz)
    Lanes_Num: int = 256           # number of lanes in the accelerator
    
    # Peak /sustained HBM bandwidth (GB/s)
    HBM_peak_BW_GBps: float = 460.0   # HBM peak bandwidth (GB/s)
    HBM_sustained_BW_GBps: float = 32.0 * 13.0 # HBM sustained bandwidth (GB/s), 13x PCIe BW, 32 AXI 

    PCIe_read_BW_GBps: float = 32.0    # PCIe Read bandwidth (GB/s)
    PCIe_write_BW_GBps: float = 32.0   # PCIe Write bandwidth (GB/s)
    
    SSD_BW_GBps: float = 7.0           # SSD Bandwidth (GB/s)
    SSD_lat_us: float = 10.0           # SSD access latency (µs)
    
    # On-chip memory capacity (MB)
    Usable_Onchip_MB: float = 35.6     
    
    # Storage model:
    # True  -> Packed by logq_bits / logp_bits
    # False -> aligned as 8 bytes / coeff
    packed_limb_storage: bool = True
    aligned_coeff_bytes: int = 8
    
    gamma_pattern: float = 1.0  # HBM access pattern penalty factor (>=1.0)
  
    #------------------------------
    # CKKS derived parameters
    #------------------------------
    
    @property
    def alpha(self) -> int:
        """alpha = ceil((L+1) / dnum)"""
        return math.ceil((self.L + 1) / self.dnum)
    
    def beta_at_level(self, level: int):
        """beta = ceil((l+1) / alpha), where level means remaining level l"""
        q = self.q_limbs_at_level(level)
        return math.ceil(q / self.alpha)

    def q_limbs_at_level(self, level: int) -> int:
        """
        level is the CURRENT remaining multiplicative level l
        fresh ct at level=L has q_limbs = L + 1
        """
        l = max(0, min(self.L, int(level)))
        return l + 1    
    
    @property
    def L_plus_k(self) -> int:
        """Total limbs in QP basis at fresh level"""
        return (self.L + 1) + self.k
    
    #------------------------------
    # Limb / ciphertext size
    #------------------------------
    @property
    def limb_bytes(self) -> int:
        """Byte size of one polynomial limb"""
        if self.packed_limb_storage:
            return math.ceil(self.logq_bits * self.N / 8)
        else:
            return self.N * self.aligned_coeff_bytes
        
    @property
    def p_limb_bytes(self) -> int:
        """Byte size of one p-limb"""
        if self.packed_limb_storage:
            return math.ceil(self.logp_bits * self.N / 8)
        else:
            return self.N * self.aligned_coeff_bytes
        
    def ciphertext_limbs(self, q_limbs: int | None = None, ct_components: int = 2) -> int:
        q = self.q_limbs_at_level(self.L) if q_limbs is None else max(1, int(q_limbs))
        comps = max(1, int(ct_components))
        return q * comps
    
    def ciphertext_bytes(self, q_limbs: int | None = None, ct_components: int = 2) -> int:
        return self.ciphertext_limbs(q_limbs, ct_components) * self.limb_bytes
    
    @property
    def onchip_capacity_limbs(self) -> int:
        """On-chip memory capacity in limbs."""
        return int((self.Usable_Onchip_MB * 1024 * 1024) / self.limb_bytes)
    
    @property
    def vectors_per_limb(self) -> int:
        """How many dp-wide vectors per limbs."""
        return math.ceil(self.N / self.Lanes_Num)
    
    #------------------------------
    # Bandwidth model
    #------------------------------
    def hbm_read_time_us(self, nbytes: float) -> float:
        """Calculates HBM read time (µs) for nbytes."""
        if nbytes <= 0:
            return 0.0
        return (nbytes / (self.HBM_peak_BW_GBps * 1e9)) * 1e6 * self.gamma_pattern

    def hbm_write_time_us(self, nbytes: float) -> float:
        """Calculates HBM write time (µs) for nbytes."""
        if nbytes <= 0:
            return 0.0
        return (nbytes / (self.HBM_peak_BW_GBps * 1e9)) * 1e6 * self.gamma_pattern

    def hbm_io_time_us(self, read_bytes: float = 0.0, write_bytes: float = 0.0) -> float:
        """Calculates total HBM read and write time (µs)."""
        return self.hbm_read_time_us(read_bytes) + self.hbm_write_time_us(write_bytes)

    def hbm_time_us(self, nbytes: float) -> float:
        """Backward compatibility: calculates HBM transfer time (µs) using the smaller of read and write bandwidths."""
        if nbytes <= 0:
            return 0.0
        return (nbytes / (self.HBM_BW_GBps * 1e9)) * 1e6 * self.gamma_pattern


    @property
    def PCIe_BW_GBps(self) -> float:
        """Backward compatibility: returns the smaller value of PCIe read and write bandwidths."""
        return min(self.PCIe_read_BW_GBps, self.PCIe_write_BW_GBps)

    def pcie_read_time_us(self, nbytes: float) -> float:
        """Calculates PCIe read time (µs) for nbytes."""
        if nbytes <= 0:
            return 0.0
        return (nbytes / (self.PCIe_read_BW_GBps * 1e9)) * 1e6

    def pcie_write_time_us(self, nbytes: float) -> float:
        """Calculates PCIe write time (µs) for nbytes."""
        if nbytes <= 0:
            return 0.0
        return (nbytes / (self.PCIe_write_BW_GBps * 1e9)) * 1e6

    def pcie_time_us(self, nbytes: float) -> float:
        """Backward compatibility: calculates PCIe transfer time (µs) using the smaller of read and write bandwidths."""
        if nbytes <= 0:
            return 0.0
        return (nbytes / (self.PCIe_BW_GBps * 1e9)) * 1e6


    def ssd_time_us(self, nbytes: float, n_accesses: int = 1) -> float:
        """Calculates SSD read time (µs) for nbytes, including access latency."""
        if nbytes <= 0:
            return 0.0
        transfer = (nbytes / (self.SSD_BW_GBps * 1e9)) * 1e6
        lat = self.SSD_lat_us * n_accesses
        return transfer + lat
