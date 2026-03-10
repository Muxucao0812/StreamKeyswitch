"""
Primitive Level for FHE
NTT: Number Theoretic Transform
Basis Conversion: QP -> Q, QP -> P, etc.
Add: Modular addition of polynomials
Mul: Modular multiplication of polynomials
Automorphism: Galois automorphism for polynomial transformation
"""

from inspect import Parameter


class Primitive:
    """Primitive operations for FHE schemes, including NTT, basis conversion, addition, multiplication, and automorphism."""
    def __init__(self, params: Parameter):
        self.params = params
        
    def basis_conversion(self):
        """Basis conversion algorithms, such as QP -> Q, QP -> P, etc."""
        raise NotImplementedError("Basis conversion method not implemented.")
    
    def ntt(self):
        """Four Step NTT algorithm implementation."""
        raise NotImplementedError("NTT method not implemented.")
    
    def add(self):
        """Modular addition of polynomials."""
        raise NotImplementedError("Addition method not implemented.")
    
    def mul(self):
        """Modular multiplication of polynomials."""
        raise NotImplementedError("Multiplication method not implemented.")
    
    def automorphism(self):
        """Galois automorphism for polynomial transformation."""
        raise NotImplementedError("Automorphism method not implemented.")
    
    