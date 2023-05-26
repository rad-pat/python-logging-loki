# -*- coding: utf-8 -*-

import abc
import collections
import copy
import functools
import logging
import time
from logging.config import ConvertingDict
from typing import Any, Dict, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from logging_loki import const
from logging_loki.config import BATCH_EXPORT_MIN_SIZE

BasicAuth = Optional[Tuple[str, str]]

MAX_REQUEST_RETRIES = 3
RETRY_BACKOFF_FACTOR = 1
RETRY_ON_STATUS = [408, 429, 500, 502, 503, 504]


logging.basicConfig(
    format="time=%(asctime)s level=%(levelname)s caller=%(module)s:%(funcName)s:%(lineno)d msg=%(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%s",
)

logger = logging


class LokiEmitter(abc.ABC):
    """Base Loki emitter class."""

    backup_buffer = collections.deque([])
    success_response_code = const.success_response_code
    level_tag = const.level_tag
    logger_tag = const.logger_tag
    label_allowed_chars = const.label_allowed_chars
    label_replace_with = const.label_replace_with
    session_class = requests.Session

    def __init__(self, url: str, tags: Optional[dict] = None, auth: BasicAuth = None):
        """
        Create new Loki emitter.

        Arguments:
            url: Endpoint used to send log entries to Loki (e.g. `https://my-loki-instance/loki/api/v1/push`).
            tags: Default tags added to every log record.
            auth: Optional tuple with username and password for basic HTTP authentication.

        """
        #: Tags that will be added to all records handled by this handler.
        self.tags = tags or {}
        #: Loki JSON push endpoint (e.g `http://127.0.0.1/loki/api/v1/push`)
        self.url = url
        #: Optional tuple with username and password for basic authentication.
        self.auth = auth

        self._session: Optional[requests.Session] = None

    def __call__(self, record: logging.LogRecord, line: str):
        """Send log record to Loki."""
        payload = self.build_payload(record, line)
        resp = self.session.post(self.url, json=payload)
        # TODO: Enqueue logs instead of raise an error that lose the logs
        if resp.status_code != self.success_response_code:
            raise ValueError(
                "Unexpected Loki API response status code: {0}".format(resp.status_code)
            )

    @abc.abstractmethod
    def build_payload(self, record: logging.LogRecord, line) -> dict:
        """Build JSON payload with a log entry."""
        raise NotImplementedError  # pragma: no cover

    @property
    def session(self) -> requests.Session:
        """Create HTTP session."""
        if self._session is None:
            self._session = self.session_class()
            self._session.auth = self.auth or None
            retry = Retry(
                total=MAX_REQUEST_RETRIES,
                backoff_factor=RETRY_BACKOFF_FACTOR,
                status_forcelist=RETRY_ON_STATUS,
            )
            self._session.mount(self.url, HTTPAdapter(max_retries=retry))

        return self._session

    def close(self):
        """Close HTTP session."""
        if self._session is not None:
            self._session.close()
            self._session = None

    @functools.lru_cache(const.format_label_lru_size)
    def format_label(self, label: str) -> str:
        """
        Build label to match prometheus format.

        `Label format <https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels>`_
        """
        for char_from, char_to in self.label_replace_with:
            label = label.replace(char_from, char_to)
        return "".join(char for char in label if char in self.label_allowed_chars)

    def build_tags(self, record: logging.LogRecord) -> Dict[str, Any]:
        """Return tags that must be sent to Loki with a log record."""
        tags = dict(self.tags) if isinstance(self.tags, ConvertingDict) else self.tags
        tags = copy.deepcopy(tags)
        tags[self.level_tag] = record.levelname.lower()
        tags[self.logger_tag] = record.name

        extra_tags = getattr(record, "tags", {})
        if not isinstance(extra_tags, dict):
            return tags

        for tag_name, tag_value in extra_tags.items():
            cleared_name = self.format_label(tag_name)
            if cleared_name:
                tags[cleared_name] = tag_value

        return tags

    def add_to_backup_buffer(self, record: dict) -> None:
        """Add record to the backup buffer that couldn't be exported to Loki"""
        logger.info("Adding elements to the back buffer queue")
        self.backup_buffer.appendleft(record)

    def is_backup_buffer_empty(self) -> bool:
        return not bool(self.backup_buffer)

    def empty_backup_buffer(self) -> None:
        """Export the backup buffer records to Loki if the service is already available"""
        idx = 1
        while True:
            try:
                logger.info(f"Draining backup buffer queue ({idx})")
                record = self.backup_buffer.pop()
                idx += 1
            except IndexError:
                logger.info("Backup queue is empty")
                return
            res = self.session.post(
                self.url,
                json=record,
            )
            if res.status_code != const.success_response_code:
                logger.error(
                    f"Loki service is still not available. Status Code {res.status_code}"
                )
                logger.info("Inserting record again into the queue")
                self.add_to_backup_buffer(record)
                return


class LokiSimpleEmitter(LokiEmitter):
    def build_payload(self, record: logging.LogRecord, line) -> dict:
        """Build JSON payload with a log entry."""
        labels = self.build_tags(record)
        ns = 1e9
        ts = str(int(time.time() * ns))
        stream = {
            "stream": labels,
            "values": [[ts, line]],
        }
        return {"streams": [stream]}


class LokiBatchEmitter(LokiEmitter):
    buffer = collections.deque([])

    def __call__(self, record: logging.LogRecord, line: str):
        """Send log record to Loki."""
        payload = self.build_payload(record, line)
        if len(self.buffer) < BATCH_EXPORT_MIN_SIZE:
            self.buffer.appendleft(payload["streams"][0])
        else:
            logger.info("Exporting logs to loki")
            logs_to_export = {
                "streams": [self.buffer.pop() for _ in range(BATCH_EXPORT_MIN_SIZE)]
            }
            resp = self.session.post(self.url, json=logs_to_export)

            if resp.status_code != self.success_response_code:
                logger.error("Failed to export logs to Loki")
                self.add_to_backup_buffer(logs_to_export)

            if not self.is_backup_buffer_empty():
                self.empty_backup_buffer()

    def _drain_queue(self):
        try:
            logs_to_export = {
                "streams": [self.buffer.pop() for _ in range(BATCH_EXPORT_MIN_SIZE)]
            }
            self.session.post(self.url, json=logs_to_export)

        except IndexError:

            return

    def build_payload(self, record: logging.LogRecord, line) -> dict:
        """Build JSON payload with a log entry."""
        labels = self.build_tags(record)
        ns = 1e9
        ts = str(int(time.time() * ns))
        stream = {
            "stream": labels,
            "values": [[ts, line]],
        }
        return {"streams": [stream]}
