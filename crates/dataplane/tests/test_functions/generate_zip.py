"""Generate application code ZIP using the tensorlake SDK.

Usage: python generate_zip.py <output_path>
Produces a ZIP file identical to what the real system uses.
"""
import hashlib
import json
import os
import sys

# Import the function to register it in the tensorlake registry
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import hello_world  # noqa: triggers @function() registration

from tensorlake.applications.registry import get_functions
from tensorlake.applications.remote.code.zip import zip_code

code_dir = os.path.dirname(os.path.abspath(__file__))
zip_bytes = zip_code(
    code_dir_path=code_dir,
    ignored_absolute_paths={os.path.abspath(__file__)},  # exclude self
    all_functions=get_functions(),
)

output_path = sys.argv[1]
with open(output_path, "wb") as f:
    f.write(zip_bytes)

# Also write metadata as JSON for the Rust test to read
meta = {
    "size": len(zip_bytes),
    "sha256": hashlib.sha256(zip_bytes).hexdigest(),
}
with open(output_path + ".meta.json", "w") as f:
    json.dump(meta, f)

print(f"Generated {output_path} ({len(zip_bytes)} bytes, sha256={meta['sha256']})")
