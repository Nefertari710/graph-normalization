# KGNF Check Results for the Running Example

Script: [`check_kgnf_running_example.py`](../scripts/check_kgnf_running_example.py).
The script checks whether the fixed `OrderInfo` feature set and key can reach KGNF through admissible decompositions under Definition 4.4, as implemented in the script.

| Dependency set | Result | States explored |
| --- | --- | --- |
| `Σ = {φ}` | KGNF is reachable via a sequence of 3 decompositions | 4 |
| `Σ = {φ₁, φ₂}` | No admissible decomposition sequence reaching KGNF was found | 4 |

Decomposition sequence found for `Σ = {φ}` (each step is written as `X → Y`):

1. Decompose `OrderInfo` along `{customer, orderDate, orderTime} → {address}`, creating reference `ρ₁`.
2. Decompose the remaining type along `{ρ₁} → {paymentMethod}`, creating reference `ρ₂`.
3. Decompose the remaining type along `{ρ₂} → {shipper}`, reaching KGNF.

These results apply only to this running example, the two given dependency sets, and the decomposition rules implemented in the script.

To reproduce the results, run from the project root:

```bash
python theory/scripts/check_kgnf_running_example.py
```
