import datetime
import pytest
from dartobsgen import ObsGenConfig, ObsSeqSource
from dartobsgen.generate import _make_analysis_windows, _format_timestamp


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


def windows(config):
    return _make_analysis_windows(
        config.start, config.end, config.assimilation_frequency
    )


def test_obs_gen_config(config):
    assert config.assimilation_frequency == datetime.timedelta(hours=6)


def test_analysis_windows_count(config):
    # start is the model run start, so the first analysis is at start + freq
    # and the last is at end: 06Z, 12Z, 18Z, 00Z.
    assert len(windows(config)) == 4


def test_first_analysis_is_one_period_after_start(config):
    first_t, _, _ = windows(config)[0]
    assert first_t == config.start + config.assimilation_frequency


def test_last_analysis_is_end(config):
    last_t, _, _ = windows(config)[-1]
    assert last_t == config.end


def test_windows_centered_on_analysis_time(config):
    half = config.assimilation_frequency / 2
    for t, date0, date1 in windows(config):
        assert date0 == t - half
        assert date1 == t + half


def test_windows_contiguous_and_non_overlapping(config):
    ws = windows(config)
    for (_, _, w1_end), (_, w2_start, _) in zip(ws, ws[1:]):
        # Shared boundary instant; the closed upper bound assigns it to the
        # earlier window, so no observation lands in two files.
        assert w1_end == w2_start


def test_filename_uses_analysis_time(config):
    first_t, _, _ = windows(config)[0]
    ts = _format_timestamp(first_t, config.output_timestamp_format)
    # 06Z = 21600 seconds of day, per DART's naming convention
    assert ts == "2010-05-01-21600"


def test_odd_frequency_rejected(config):
    with pytest.raises(ValueError, match="even whole number of seconds"):
        _make_analysis_windows(
            config.start, config.end, datetime.timedelta(seconds=3)
        )


def test_obs_seq_source_stub():
    src = ObsSeqSource("/tmp")
    with pytest.raises(NotImplementedError):
        src.write_obs_seq("x", None, None, None, 0, 0, 0, 0, [], None)
