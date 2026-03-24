"""Quick sanity-check for _to_dart_time."""
import pandas as pd
from datetime import datetime
from dartobsgen.sources.nnja import _to_dart_time

# Test 1: 2021-01-01 00:00 UTC → 153402 days, 0 seconds (verified with Python datetime)
ts = pd.Series(pd.to_datetime(["2021-01-01 00:00:00"]).tz_localize("UTC"))
days, secs = _to_dart_time(ts)
print(f"2021-01-01 00:00 UTC → days={days.iloc[0]}, seconds={secs.iloc[0]}")
assert days.iloc[0] == 153402, f"expected 153402, got {days.iloc[0]}"
assert secs.iloc[0] == 0
print("Test 1 passed")

# Test 2: 2021-01-01 06:00 UTC → same day, 21600 seconds
ts2 = pd.Series(pd.to_datetime(["2021-01-01 06:00:00"]).tz_localize("UTC"))
d2, s2 = _to_dart_time(ts2)
assert d2.iloc[0] == 153402 and s2.iloc[0] == 21600
print("Test 2 passed")

# Test 3: naive timestamps treated as-is
ts3 = pd.Series(pd.to_datetime(["2021-01-01 12:00:00"]))
d3, s3 = _to_dart_time(ts3)
assert d3.iloc[0] == 153402 and s3.iloc[0] == 43200
print("Test 3 passed")

print("\nAll DART time conversion tests passed")
