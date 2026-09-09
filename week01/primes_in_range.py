from __future__ import annotations
import math
from typing import List

def get_primes(low: int, high: int, *, return_list: bool = True) -> List[int] | int:
    """
    Find primes in [low, high) using a segmented sieve.
    If return_list=True -> returns List[int]
    Else -> returns count (int)

    Good for: large ranges, parallel/distributed chunking.
    """
    if high <= 2 or high <= low:
        return [] if return_list else 0

    low = max(low, 2)

    # 1) base primes up to sqrt(high-1)
    limit = int(math.isqrt(high - 1))
    base = bytearray(b"\x01") * (limit + 1)
    base[0:2] = b"\x00\x00"
    for p in range(2, int(limit**0.5) + 1):
        if base[p]:
            base[p*p:limit+1:p] = b"\x00" * (((limit - p*p)//p) + 1)
    base_primes = [i for i in range(limit + 1) if base[i]]

    # 2) segmented sieve for [low, high)
    size = high - low
    seg = bytearray(b"\x01") * size

    for p in base_primes:
        pp = p * p
        if pp >= high:
            break
        start = (low + p - 1) // p * p
        if start < pp:
            start = pp
        for x in range(start, high, p):
            seg[x - low] = 0

    if return_list:
        return [low + i for i, is_p in enumerate(seg) if is_p]
    else:
        return int(sum(seg))  # seg is 1 for prime, 0 for composite
