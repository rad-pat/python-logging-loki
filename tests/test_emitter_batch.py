import collections
import logging
from unittest.mock import MagicMock

import pytest

from logging_loki.emitter import LokiBatchEmitter
from logging_loki.config import BATCH_EXPORT_MIN_SIZE

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

    yield instance, session
    instance._drain_queue()


def create_record(**kwargs) -> logging.LogRecord:
    """Create test logging record."""
    log = logging.Logger(__name__)
    return log.makeRecord(**{**record_kwargs, **kwargs})


def test_record_added_to_buffer(emitter_batch):
    emitter, _ = emitter_batch
    for _ in range(10):
        emitter(create_record(), "")

    assert len(emitter.buffer) == 10


def test_buffer_is_drained_if_size_is_above_batch_size_cfg(emitter_batch):
    total_records = 20
    emitter, session = emitter_batch
    assert len(emitter.buffer) == 0
    for _ in range(total_records):
        emitter(create_record(), "")
    assert len(emitter.buffer) == total_records - BATCH_EXPORT_MIN_SIZE - 1
