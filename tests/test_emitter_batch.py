import logging
import pytest
from logging_loki.emitter import LokiBatchEmitter
from unittest.mock import MagicMock

emitter_url: str = "https://example.net/loki/api/v1/push/"
record_kwargs = {
    "name": "test",
    "level": logging.WARNING,
    "fn": "",
    "lno": "",
    "msg": "Test",
    "args": None,
    "exc_info": None,
}


@pytest.fixture()
def emitter_batch():
    """Create v1 emitter with mocked http session."""
    response = MagicMock()
    response.status_code = LokiBatchEmitter.success_response_code
    session = MagicMock()
    session().post = MagicMock(return_value=response)

    instance = LokiBatchEmitter(url=emitter_url)
    instance.session_class = session

    return instance, session


def create_record(**kwargs) -> logging.LogRecord:
    """Create test logging record."""
    log = logging.Logger(__name__)
    return log.makeRecord(**{**record_kwargs, **kwargs})


def test_raises_value_error_on_non_successful_response(emitter_batch):
    emitter, session = emitter_batch
    session().post().status_code = None

    with pytest.raises(ValueError):
        emitter(create_record(), "")
        pytest.fail(
            "Must raise ValueError on non-successful Loki response"
        )  # pragma: no cover
