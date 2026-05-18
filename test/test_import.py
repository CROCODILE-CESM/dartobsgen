import datetime
import pytest
from dartobsgen import ObsGenConfig, ObsSeqSource
from dartobsgen.generate import _make_windows, _format_timestamp


@pytest.fixture
def config():
    return ObsGenConfig(
        start=datetime.datetime(2010, 5, 1),
        end=datetime.datetime(2010, 5, 2),
        lat_min=5, lat_max=60,
        lon_min=-100, lon_max=-30,
        obs_types=["ARGO_TEMPERATURE", "ARGO_SALINITY"],
        assimilation_frequency=datetime.timedelta(hours=6),
    )


def test_obs_gen_config(config):
    assert config.assimilation_frequency == datetime.timedelta(hours=6)


def test_make_windows_count(config):
    windows = _make_windows(config.start, config.end, config.assimilation_frequency)
    assert len(windows) == 4


def test_make_windows_no_overlap(config):
    windows = _make_windows(config.start, config.end, config.assimilation_frequency)
    for (_, w1), (w2, _) in zip(windows, windows[1:]):
        assert w1 == w2


def test_format_timestamp(config):
    windows = _make_windows(config.start, config.end, config.assimilation_frequency)
    ts = _format_timestamp(windows[0][0], config.output_timestamp_format)
    assert isinstance(ts, str)
    assert len(ts) > 0


def test_obs_seq_source_stub():
    src = ObsSeqSource("/tmp")
    with pytest.raises(NotImplementedError):
        src.write_obs_seq("x", None, None, 0, 0, 0, 0, [], None)
