"""
Operation Level Latency Estimation 
Main operations: add, mul, automorphism, keyswitch
"""

from enum import StrEnum
from typing import Literal
from dataclasses import dataclass
from parameter import FHEParameter
from ciphertext import Ciphertext, Plaintext
from primitive import Primitive
from memory_model import estimate_io_and_spill_us

OpName = Literal[
    "cadd", 
    "padd", 
    "cmult", 
    "pmult",
    "keyswitch",
    "rotate",
    "rescale"
]

@dataclass(frozen=True)
class OperationBreakdown:
    total_us: float
    compute_us: float
    io_us: float
    spill_us: float
    bottleneck: str
    compute_ratio: float
    io_ratio: float
    detail: dict
    
    
class Operation:
    """
    CGRA-style operation model:
    - cadd/padd/pmult: Streaming Bandwith dominated
    - cmult/rotate: Domained by Keyswitch pipeline
    - rescale can be merged into moddown inside mult
    """

    def __init__(self, params: FHEParameter):
        self.params = params
        self.primitive = Primitive(params)
        
    def _finalize(
        self,
        op: OpName,
        compute_us: float,
        mem,
        detail: dict,
    ) -> OperationBreakdown:
        body_us = max(compute_us, mem.io_us)
        total_us = body_us + mem.spill_us
        
        dominant = max(compute_us, mem.io_us)
        if dominant <= 0:
            bottleneck = "none"
            compute_ratio = 0.0
            io_ration = 0.0
        else:
            compute_ratio = compute_us / dominant
            io_ration = mem.io_us / dominant
            if compute_us > mem.io_us * 1.2:
                bottleneck = "compute-bound"
            elif mem.io_us > compute_us * 1.2:
                bottleneck = "bandwidth-bound"
            else:
                bottleneck = "balanced"
            
        
        return OperationBreakdown(
            total_us=total_us,
            compute_us=compute_us,
            io_us=mem.io_us,
            spill_us=mem.spill_us,
            bottleneck=bottleneck,
            compute_ratio=compute_ratio,
            io_ratio=io_ration,
            detail={
                "op": op,
                **detail,
                "read_limbs": mem.read_limbs,
                "write_limbs": mem.write_limbs,
                "read_bytes": mem.read_bytes,
                "write_bytes": mem.write_bytes,
                "spill_limbs": mem.spill_limbs,
                "spill_bytes": mem.spill_bytes,
            },
        )
    
    def _merge_stages(
        self,
        op_name:str,
        stages: list[OperationBreakdown],
        extra_detail: dict | None = None,
    ) -> OperationBreakdown:
        extra_detail = extra_detail or {}
        total_us = sum(stage.total_us for stage in stages)
        compute_us = sum(stage.compute_us for stage in stages)
        io_us = sum(stage.io_us for stage in stages)
        spill_us = sum(stage.spill_us for stage in stages)
        
        dominant = max(compute_us, io_us)
        if dominant <= 0:
            bottleneck = "none"
            compute_ratio = 0.0
            io_ratio = 0.0
        else:            
            compute_ratio = compute_us / dominant
            io_ratio = io_us / dominant
            if compute_us > io_us * 1.2:
                bottleneck = "compute-bound"
            elif io_us > compute_us * 1.2:
                bottleneck = "bandwidth-bound"
            else:
                bottleneck = "balanced"
        detail = {
            "op": op_name,
            "num_stages": len(stages),
            "stage_names": [stage.detail.get("op", f"stage_{i}") for i, stage in enumerate(stages)],
            "stages": [stage.detail for stage in stages],
            **extra_detail,
        }

        return OperationBreakdown(
            total_us=total_us,
            compute_us=compute_us,
            io_us=io_us,
            spill_us=spill_us,
            bottleneck=bottleneck,
            compute_ratio=compute_ratio,
            io_ratio=io_ratio,
            detail=detail,
        )
        
    # ------------------------------
    # public API
    # ------------------------------
    def estimate(
        self,
        op: OpName,
        lhs: Ciphertext,
        rhs: Ciphertext | Plaintext | None = None,
        input_onchip: bool = False,
        output_onchip: bool = False,
    ) -> OperationBreakdown:
        
        if op == "cadd":
            assert isinstance(rhs, Ciphertext), "cadd requires ciphertext rhs"
            OpBreakdown = self.estimate_latency_cadd(
                lhs, 
                rhs, 
                input_onchip=input_onchip,
                output_onchip=output_onchip
            )
            return OpBreakdown
        
        if op == "padd":
            assert isinstance(rhs, Plaintext), "padd requires plaintext rhs"
            OpBreakdown = self.estimate_latency_padd(
                lhs, 
                rhs, 
                input_onchip=input_onchip, 
                output_onchip=output_onchip
            )
            return OpBreakdown
        
        if op == "cmult":
            assert isinstance(rhs, Ciphertext), "cmult requires ciphertext rhs"
            OpBreakdown = self.estimate_latency_cmult(
                lhs, 
                rhs, 
                input_onchip=input_onchip, 
                output_onchip=output_onchip
            )
            return OpBreakdown
        
        if op == "pmult":
            assert isinstance(rhs, Plaintext), "pmult requires plaintext rhs"
            OpBreakdown = self.estimate_latency_pmult(
                lhs, 
                rhs, 
                input_onchip=input_onchip, 
                output_onchip=output_onchip
            )
            return OpBreakdown
        
        if op == "rotate":
            OpBreakdown = self.estimate_latency_rotate(
                lhs,
                input_onchip=input_onchip, 
                output_onchip=output_onchip
            )
            return OpBreakdown
        
        if op == "rescale":
            OpBreakdown = self.estimate_latency_rescale(
                lhs, 
                input_onchip=input_onchip,
                output_onchip=output_onchip
            )
            return OpBreakdown
        
        if op == "keyswitch":
            raise NotImplementedError("keyswitch latency estimation not implemented yet")
        
        raise ValueError(f"unsupported op: {op}")
    
    def estimate_latency_cadd(
        self,
        lhs: Ciphertext,
        rhs: Ciphertext,
        input_onchip: bool = False,
        output_onchip: bool = False,
    ) -> OperationBreakdown:
        """
        Estimate latency for cadd
        Ct + Ct = Ct
        """
        q = min(lhs.q_limbs(self.params), rhs.q_limbs(self.params))
        comps = max(lhs.ct_components, rhs.ct_components)
        
        read_limbs = q * lhs.ct_components + q * rhs.ct_components
        write_limbs = q * comps
        
        mem = estimate_io_and_spill_us(
            self.params,
            read_limbs=read_limbs,
            write_limbs=write_limbs,
            input_onchip=input_onchip,
            output_onchip=output_onchip,
            working_set_limbs=min(read_limbs + write_limbs, 8),
        )
        compute_us = q * comps * self.primitive.add_latency
        
        return self._finalize(
            op="cadd",
            compute_us=compute_us,
            mem=mem,
            detail={
                "q_limbs": q,
                "components": comps,
                "input_onchip": input_onchip,
                "output_onchip": output_onchip,
            }
        )
        
    def estimate_latency_padd(
        self,
        lhs: Ciphertext,
        rhs: Plaintext,
        input_onchip: bool = False,
        output_onchip: bool = False
    ) -> OperationBreakdown:
        """
        Estimate latency for padd
        Ct + Pt = Ct
        """
        q = lhs.q_limbs(self.params)
        comps = lhs.ct_components

        read_limbs = q * lhs.ct_components + q * rhs.pt_components
        write_limbs = q * comps

        mem = estimate_io_and_spill_us(
            self.params,
            read_limbs=read_limbs,
            write_limbs=write_limbs,
            input_onchip=input_onchip,
            output_onchip=output_onchip,
            working_set_limbs=min(read_limbs + write_limbs, 8),
        )
        compute_us = q * comps * self.primitive.add_latency

        return self._finalize(
            op="padd",
            compute_us=compute_us,
            mem=mem,
            detail={
                "q_limbs": q,
                "components": comps,
                "input_onchip": input_onchip,
                "output_onchip": output_onchip,
            }
        )
        
    def estimate_latency_pmult_core(
        self,
        lhs: Ciphertext,
        rhs: Plaintext,
        input_onchip: bool = False,
        output_onchip: bool = False,
        with_rescale: bool = False,
        fused_rescale: bool = False,
    ) -> OperationBreakdown:
        """
        PMult core only, without standalone rescale
        Ct * Pt = Ct
        """
        q = lhs.q_limbs(self.params)
        comps = lhs.ct_components

        read_limbs = q * lhs.ct_components + q * rhs.pt_components
        write_limbs = q * comps

        mem = estimate_io_and_spill_us(
            self.params,
            read_limbs=read_limbs,
            write_limbs=write_limbs,
            input_onchip=input_onchip,
            output_onchip=output_onchip,
            working_set_limbs=min(read_limbs + write_limbs, 8),
        )
        compute_us = q * comps * self.primitive.mul_latency

        return self._finalize(
            op="pmult",
            compute_us=compute_us,
            mem=mem,
            detail={
                "q_limbs": q,
                "components": comps,
                "input_onchip": input_onchip,
                "output_onchip": output_onchip,
            }
        )
    
    def estimate_latency_pmult(
        self,
        lhs: Ciphertext,
        rhs: Plaintext,
        input_onchip: bool = False,
        output_onchip: bool = False,
        with_rescale: bool = False,
        fused_rescale: bool = False,
    ) -> OperationBreakdown:
        """
        Estimate latency for pmult
        Mode:
        - without_rescale: 
            return PMult core latency only, without standalone rescale
        - with_rescale=True and fused_rescale=False:
            return PMult core latency + standalone rescale latency 
        - with_rescale=True and fused_rescale=True:
            keep PMult standalone here, assume rescale will be absorbed by a higher-level fused datapath
        """
        stage1 = self.estimate_latency_pmult_core(
            lhs,
            rhs,
            input_onchip=input_onchip,
            output_onchip=(True if with_rescale and not fused_rescale else output_onchip) 
        )
        
        if not with_rescale:
            return self._merge_stages(
                op_name="pmult",
                stages=[stage1],
                extra_detail={
                    "with_rescale": with_rescale,
                    "fused_rescale": fused_rescale,
                }
            )
                
        if fused_rescale:
            # assume rescale is fully fused and does not add extra latency
            return self._merge_stages(
                op_name="pmult_fused_rescale",
                stages=[stage1],
                extra_detail={
                    "with_rescale": with_rescale,
                    "fused_rescale": fused_rescale,
                    "note": "standalone rescale not added here; assumed fused in higher-level datapath",
                }
            )
        pmult_out = Ciphertext(
            level=lhs.level,
            ct_components=lhs.ct_components,
        )
        
        stage2 = self.estimate_latency_rescale(
            lhs=pmult_out,  # rescale latency should be estimated based on the output of pmult
            input_onchip=True,  # rescale can directly consume pmult output on-chip
            output_onchip=output_onchip
        )
        
        return self._merge_stages(
            op_name="pmult",
            stages=[stage1, stage2],
            extra_detail={
                "with_rescale": with_rescale,
                "fused_rescale": fused_rescale,
            },
        )
    
            
    def estimate_latency_rescale(
        self,
        lhs: Ciphertext,
        input_onchip: bool = False,
        output_onchip: bool = False,
    ) -> OperationBreakdown:
        """
        Estimate latency for rescale
        """
        # Placeholder implementation - replace with actual rescale latency estimation
        q = lhs.q_limbs(self.params)
        comps = lhs.ct_components
        out_q = max(1, q-1)
        
        read_limbs = q * comps
        write_limbs = out_q * comps
        
        # Rescale working set should be larger than add/pmult
        print(comps)
        working_set_limbs = min(read_limbs + write_limbs, comps * 4)
        print(working_set_limbs)
        
        mem = estimate_io_and_spill_us(
            self.params,
            read_limbs=read_limbs,
            write_limbs=write_limbs,
            input_onchip=input_onchip,
            output_onchip=output_onchip,
            working_set_limbs=working_set_limbs,
        )
        
        compute_us = (
            self.primitive.intt_latency +
            self.primitive.bconv_latency +
            self.primitive.ntt_latency
        ) * comps
        
        return self._finalize(
            op="rescale",
            compute_us=compute_us,
            mem=mem,
            detail={
                "q_limbs_in": q,
                "q_limbs_out": out_q,
                "components": comps,
                "working_set_limbs": working_set_limbs,
                "input_onchip": input_onchip,
                "output_onchip": output_onchip,
            }
        )
        
    def estimate_latency_rotate(
        self,
        lhs: Ciphertext,
        input_onchip: bool = False,
        output_onchip: bool = False,
    ) -> OperationBreakdown:
        """
        Estimate latency for rotate (automorphism)
        """
        q = lhs.q_limbs(self.params)
        comps = lhs.ct_components

        read_limbs = q * comps
        write_limbs = q * comps 
        working_set_limbs = min(read_limbs + write_limbs, max(24, self.params.alpha + self.params.k + 4))
        
        mem = estimate_io_and_spill_us(
            self.params,
            read_limbs=read_limbs,
            write_limbs=write_limbs,
            input_onchip=input_onchip,
            output_onchip=output_onchip,
            working_set_limbs=working_set_limbs,
        )
        compute_us = self._estimate_keyswitch_latency(lhs, method = "default") + q * self.primitive.auto_latency
        
        return self._finalize(
            op="rotate",
            compute_us=compute_us,
            mem=mem,
            detail={
                "q_limbs": q,
                "components": comps,
                "working_set_limbs": working_set_limbs,
                "input_onchip": input_onchip,
                "output_onchip": output_onchip,
            }
        )
        
    def estimate_latency_cmult(
        self,
        
    )
                                

if __name__ == "__main__":

    params = FHEParameter()
    op = Operation(params)

    ct1 = Ciphertext(level=23, ct_components=2)
    ct2 = Ciphertext(level=23, ct_components=2)
    pt1 = Plaintext(level=23, pt_components=1)

    print(f"Add (ct + ct): {op.estimate("cadd", ct1, ct2, input_onchip=False, output_onchip=False)}")
    print("="*60)
    print(f"Add (ct + pt): {op.estimate("padd", ct1, pt1, input_onchip=False, output_onchip=False)}")
    print("="*60)
    print(f"Mult (ct * pt): {op.estimate("pmult", ct1, pt1, input_onchip=False, output_onchip=False)}")
    print("="*60)
    print(f"Rescale: {op.estimate("rescale", ct1, input_onchip=True, output_onchip=True)}")
 