import subprocess

from collections import abc

from cobald_condor_limits.query_view import CondorPoolView, QueryError


def pool_command(*base_command: str, pool: str =None) -> list:
    """Transform a query to target a specific pool"""
    if pool is None:
        return list(base_command)
    else:
        return [base_command[0]] + ['-pool', str(pool)] + list(base_command[1:])


def query_limits(query_command, key_transform):
    resource_limits = {}
    for item in subprocess.check_output(query_command, timeout=10, universal_newlines=True).splitlines():
        if '=' not in item:
            continue
        key, _, value = (value.strip() for value in item.partition('='))
        try:
            resource = key_transform(key)
        except ValueError:
            continue
        else:
            try:
                resource_limits[resource] = float(value)
            except ValueError:
                pass
    return resource_limits


class ConcurrencyConstraintView(CondorPoolView, abc.MutableMapping):
    """
    View on the ConcurrencyLimit as constraint by configuration

    Both monitor and runtime information is available in the ``condor.concurrency_limit`` subchannels.
    """
    def __init__(self, pool: str = None, max_age: float = 10):
        super().__init__(pool=pool, max_age=max_age, channel_name='condor.concurrency_limit')

    def __getitem__(self, resource: str) -> float:
        try:
            return super().__getitem__(resource)
        except KeyError:
            if '.' in resource:
                root_resource = resource.split('.')[0]
                return self._data[root_resource]  # check parent group of resource
            raise

    def __delitem__(self, key):
        self._set_constraint(key, '')
        self._valid_date = 0

    def __setitem__(self, key: str, value: float):
        self._set_constraint(key, str(int(value)))
        self._valid_date = 0

    @staticmethod
    def _key_to_resource(key: str) -> str:
        if key.lower()[-6:] == '_limit':
            return key[:-6]
        raise ValueError

    def _query_data(self):
        query_command = pool_command('condor_config_val', '-negotiator', '-dump', 'LIMIT', pool=self.pool)
        try:
            resource_limits = query_limits(query_command, key_transform=self._key_to_resource)
        except subprocess.CalledProcessError as err:
            raise QueryError from err
        else:
            return resource_limits

    def _set_constraint(self, resource: str, constraint: str):
        reconfig_command = pool_command(
            'condor_config_val', '-negotiator', '-rset', '%s_LIMIT = %s' % (resource, constraint), pool=self.pool
        )
        flush_command = pool_command('condor_reconfig', '-negotiator', pool=self.pool)
        try:
            subprocess.check_call(reconfig_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
            subprocess.check_call(flush_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            self._runtime_log.exception('failed to constraint %r to %r', resource, constraint)
        else:
            self._valid_date = 0


class ConcurrencyUsageView(CondorPoolView):
    """
    View on the ConcurrencyLimit as used by jobs in the pool

    Both monitor and runtime information is available in the ``condor.concurrency_limit`` subchannels.
    """
    def __init__(self, pool: str = None, max_age: float = 10):
        super().__init__(pool=pool, max_age=max_age, channel_name='condor.concurrency_usage')

    def __getitem__(self, resource: str) -> float:
        try:
            return super().__getitem__(resource.replace('.', '_'))
        except KeyError:
            if '.' in resource:
                return self._data[resource.split('.')[0]]  # check parent group of resource
            raise

    @staticmethod
    def _key_to_resource(key: str) -> str:
        if key.startswith('ConcurrencyLimit_'):
            return key[17:]
        raise ValueError

    def _query_data(self):
        query_command = pool_command('condor_userprio', '-negotiator', '-long', pool=self.pool)
        try:
            resource_usage = query_limits(query_command, key_transform=self._key_to_resource)
        except subprocess.CalledProcessError as err:
            raise QueryError from err
        else:
            return resource_usage


class PoolResources(CondorPoolView):
    """
    View on the processing Resources available in a pool

    The view currently supports the following resources:

    ``"cpus"``
        Number of CPU Cores (``TotalSlotCpus`` ClassAd)

    ``"memory"``
        Memory available as RAM in MiB (``TotalSlotMemory`` ClassAd)

    ``"disk"``
        Disk space available in KiB (``TotalSlotDisk`` ClassAd)

    ``"machines"``
        Number of distinct machines, as identified by their hostname.
    """
    def __init__(self, pool: str = None, max_age: float = 30):
        super().__init__(pool=pool, max_age=max_age, channel_name='condor.pool_resources')

    def _query_data(self):
        query_command = pool_command(
            'condor_status',
            "-startd",
            "-constraint", ' && '.join((
                'SlotType!="Dynamic"',  # Dynamic slots are part of entire machines which we already match
                'State=!="Owner"',  # Owner machines are unavailable
            )),
            "-af", "TotalSlotCpus", "TotalSlotMemory", "TotalSlotDisk", "Machine",
            pool=self.pool,
        )
        data = {'cpus': 0, 'memory': 0, 'disk': 0, 'machines': 0}
        machines = set()
        try:
            for machine_info in subprocess.check_output(query_command, universal_newlines=True).splitlines():
                try:
                    cpus, memory, disk, machine = machine_info.split()
                except ValueError:
                    continue
                else:
                    data['cpus'] += float(cpus)
                    data['memory'] += float(memory)
                    data['disk'] += float(disk)
                    machines.add(machine)
        except subprocess.CalledProcessError:
            pass
        else:
            data['machines'] = len(machines)
            return data


class PoolResourceView(object):
    """View on a single resource in the pool"""
    def __init__(self, resource: str, pool_resources: PoolResources):
        self._resource = resource
        self._pool_resources = pool_resources

    def __int__(self):
        return int(self._pool_resources[self._resource])

    def __float__(self):
        return float(self._pool_resources[self._resource])

    def __sub__(self, other):
        return float(self) - other


__all__ = ["ConcurrencyConstraintView", "ConcurrencyUsageView", "PoolResources", "PoolResourceView"]
