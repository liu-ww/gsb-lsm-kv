# Ensures the repository root is importable when running pytest from a
# different working directory / without an installed package.
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
