import time
import logging
from collections import abc


class QueryError(Exception):
    pass


class CondorPoolView(abc.Mapping):
    """
    View on information in an HTCondor pool

    :param pool: the pool to query for information
    :param max_age: maximum age of information before refreshing
    :param channel_name: trailing name for ``runtime`` and ``monitor`` channels
    :param monitor_identifier: identifier used for monitoring messages
    """
    def __init__(self, pool: str = None, max_age: float = 10, channel_name: str = 'condor_query'):
        self.pool = pool
        self.max_age = max_age
        self._valid_date = 0
        self._data = {}
        self._runtime_log = logging.getLogger('cobald.runtime.%s' % channel_name)
        self._monitor_log = logging.getLogger('cobald.monitor.%s' % channel_name)

    def __len__(self):
        self._try_refresh()
        return len(self._data)

    def __iter__(self):
        self._try_refresh()
        return iter(self._data)

    def __getitem__(self, item):
        self._try_refresh()
        return self._data[item]

    def _try_refresh(self):
        if self._valid_date < time.time():
            self._runtime_log.debug('Querying pool %r ...', self.pool)
            try:
                data = self._query_data()
            except QueryError:
                self._runtime_log.exception('Querying pool %r failed', self.pool)
            else:
                self._monitor_log.debug('Querying pool %r result: %s', self.pool, data)
                self._data = data
                self._valid_date = self.max_age + time.time()

    @staticmethod
    def _query_data():
        raise NotImplementedError

    def __str__(self):
        self._try_refresh()
        return str(self._data)

    def __repr__(self):
        return '<%s, pool=%s, max_age=%s, log_id=%s, _valid_date=%s, _data=%r>' % (
            self.__class__.__name__, self.pool, self.max_age, self._monitor_log.name[14:],
            self._data, self._valid_date,
        )
