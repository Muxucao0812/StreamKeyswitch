import math

class FHEParameter:
    """FHE parameters"""
    
    # Parameters for the FHE scheme
    N: int = 65536      # 多项式的阶
    W_bytes: int = 8    # 每个系数的字节数
    Q_limbs: int = 27   # 模数 Q 的 limb 数量
    k_limbs: int = 4   # 模数 k 的 limb 数量
    alpha: int = 4      # 每个digital的limb数量
    
    # Parameters for the FPGA 
    BRAM_Capacity_MB: int = 540  # FPGA BRAM 容量 (MB)
    HBM_read_BW_GBps: float = 460.0    # HBM 读带宽 (GB/s)
    HBM_write_BW_GBps: float = 460.0   # HBM 写带宽 (GB/s)
    PCIe_read_BW_GBps: float = 32.0    # PCIe 读带宽 (GB/s)
    PCIe_write_BW_GBps: float = 32.0   # PCIe 写带宽 (GB/s)
    SSD_BW_GBps: float = 7.0           # SSD 带宽 (GB/s)
    SSD_lat_us: float = 10.0           # SSD 单次访问延迟 (µs)
    
    # Parameters for the access pattern
    gamma_pattern: float = 1.0         # 访问模式因子
    
    # Parameters for the accelerator design
    Lanes_Num: int = 256                   # 加速器的 lane 数量
    SPU_Num : int = 4
    
    @property
    def l(self) -> int:
        """l = Q_limbs - 1，用于计算程度下降后的 limb 数。"""
        return self.Q_limbs - 1
    
    @property
    def beta(self) -> int:
        """digital 数量 beta = ceil(Q_limbs / alpha)，用于计算程度下降后的 digital 数。"""
        return math.ceil(self.Q_limbs / self.alpha)
    
    @property
    def limb_bytes(self) -> int:
        """每个 limb 的字节数，等于 N * W_bytes。"""
        return self.N * self.W_bytes
    
    @property
    def L_plus_k(self) -> int:
        """QP 基底的总limb数量，等于 Q_limbs + k_limbs。"""
        return self.Q_limbs + self.k_limbs
    
    
    @property
    def onchip_capacity_limbs(self) -> int:
        """片上可以存储的limbs的数量"""
        onchip_bytes = self.BRAM_Capacity_MB * 1024 * 1024  # 540MB
        return onchip_bytes // self.limb_bytes
    

    @property
    def PCIe_BW_GBps(self) -> float:
        """向后兼容：返回 PCIe 读写带宽的较小值。"""
        return min(self.PCIe_read_BW_GBps, self.PCIe_write_BW_GBps)

    # ── HBM 方向性传输时间 ──
    def hbm_read_time_us(self, nbytes: float) -> float:
        """计算 nbytes 的 HBM 读取时间（µs）。"""
        if nbytes <= 0:
            return 0.0
        return (nbytes / (self.HBM_read_BW_GBps * 1e9)) * 1e6 * self.gamma_pattern

    def hbm_write_time_us(self, nbytes: float) -> float:
        """计算 nbytes 的 HBM 写入时间（µs）。"""
        if nbytes <= 0:
            return 0.0
        return (nbytes / (self.HBM_write_BW_GBps * 1e9)) * 1e6 * self.gamma_pattern

    def hbm_time_us(self, nbytes: float) -> float:
        """向后兼容：使用 HBM 读写带宽较小值计算传输时间（µs）。"""
        if nbytes <= 0:
            return 0.0
        return (nbytes / (self.HBM_BW_GBps * 1e9)) * 1e6 * self.gamma_pattern

    # ── PCIe 方向性传输时间 ──
    def pcie_read_time_us(self, nbytes: float) -> float:
        """计算 nbytes 的 PCIe 读取时间（µs）。"""
        if nbytes <= 0:
            return 0.0
        return (nbytes / (self.PCIe_read_BW_GBps * 1e9)) * 1e6

    def pcie_write_time_us(self, nbytes: float) -> float:
        """计算 nbytes 的 PCIe 写入时间（µs）。"""
        if nbytes <= 0:
            return 0.0
        return (nbytes / (self.PCIe_write_BW_GBps * 1e9)) * 1e6

    def pcie_time_us(self, nbytes: float) -> float:
        """向后兼容：使用 PCIe 读写带宽较小值计算传输时间（µs）。"""
        if nbytes <= 0:
            return 0.0
        return (nbytes / (self.PCIe_BW_GBps * 1e9)) * 1e6

    # ── SSD ──
    def ssd_time_us(self, nbytes: float, n_accesses: int = 1) -> float:
        """计算 nbytes 的 SSD 读取时间（µs），含访问延迟。"""
        if nbytes <= 0:
            return 0.0
        transfer = (nbytes / (self.SSD_BW_GBps * 1e9)) * 1e6
        lat = self.SSD_lat_us * n_accesses
        return transfer + lat
