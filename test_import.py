import datetime
from dart_obs_gen import ObsGenConfig, CrocLakeSource, ObsSeqSource, DataSource, generate_obs_sequences
from dart_obs_gen.generate import _make_windows, _format_timestamp

config = ObsGenConfig(
    start=datetime.datetime(2010, 5, 1),
    end=datetime.datetime(2010, 5, 2),
    lat_min=5, lat_max=60,
    lon_min=-100, lon_max=-30,
    obs_types=["ARGO_TEMPERATURE", "ARGO_SALINITY"],
    assimilation_frequency_hours=6,
)
print("ObsGenConfig OK:", config.assimilation_frequency_hours, "h windows")

windows = _make_windows(config.start, config.end, config.assimilation_frequency_hours)
print("Windows for 1 day @ 6h:", len(windows), "windows")
for w0, w1 in windows:
    ts = _format_timestamp(w0, config.output_timestamp_format)
    print(f"  {w0.isoformat()} -> {w1.isoformat()}  => obs_seq.{ts}.out")

assert windows[0][1] == windows[1][0], "Windows overlap!"
print("No-overlap check: PASS")

src = ObsSeqSource("/tmp")
try:
    src.write_obs_seq("x", None, None, 0, 0, 0, 0, [], None)
    print("ObsSeqSource stub: FAIL (no exception)")
except NotImplementedError:
    print("ObsSeqSource stub: PASS")

print("All checks passed.")
