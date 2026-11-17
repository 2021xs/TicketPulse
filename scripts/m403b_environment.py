#!/usr/bin/env python3
"""M4-03B private runtime. Never calls setup/clone on previous environments."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import time
import urllib.request

import local_mysql as distribution_mysql
import local_search as distribution_es
import local_kafka as distribution_kafka
import bench_ck_runtime as distribution_ck
import m502_mysql as mysql_process

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / '.local/m4-03b'
OUT = ROOT / 'artifacts/m4-03b'
MYSQL = RUNTIME / 'mysql'
ES = RUNTIME / 'elasticsearch'
CK = RUNTIME / 'clickhouse'
KAFKA = RUNTIME / 'kafka'
MYSQL_PORT, ES_PORT, ES_TRANSPORT = 33312, 19206, 19306
CK_PORT, CK_NATIVE = 18129, 19007
KAFKA_PORT, CONTROLLER_PORT, APP_PORT = 19102, 19103, 18095
ES_URL, CK_URL = 'http://127.0.0.1:' + str(ES_PORT), 'http://127.0.0.1:' + str(CK_PORT)
ENV = RUNTIME / 'environment.json'


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def occupied(port):
    with socket.socket() as connection:
        return connection.connect_ex(('127.0.0.1', port)) == 0


def private_file(path, content):
    distribution_mysql.private_file(path, content)


def sha(path, algorithm='sha256'):
    digest = hashlib.new(algorithm)
    with path.open('rb') as src:
        for block in iter(lambda: src.read(1048576), b''):
            digest.update(block)
    return digest.hexdigest()


def safe_paths():
    if RUNTIME.resolve() != ROOT / '.local/m4-03b' or OUT.resolve() != ROOT / 'artifacts/m4-03b':
        raise RuntimeError('M4-03B runtime/evidence paths differ')
    if len(set([MYSQL_PORT, ES_PORT, ES_TRANSPORT, CK_PORT, CK_NATIVE, KAFKA_PORT, CONTROLLER_PORT, APP_PORT])) != 8:
        raise RuntimeError('Private ports overlap')
    if any(port in [33306, 19200, 19300, 18123, 19000, 19092, 19093, 18080]
           for port in [MYSQL_PORT, ES_PORT, ES_TRANSPORT, CK_PORT, CK_NATIVE, KAFKA_PORT, CONTROLLER_PORT, APP_PORT]):
        raise RuntimeError('Demonstration endpoint prohibited')


def budget():
    # Merge/insert temporary paths may disappear during an observation. Count
    # live regular files without following CK data symlinks or failing the run
    # because a directory was atomically renamed by its own server.
    disk = 0
    def walk_error(error):
        if not isinstance(error, FileNotFoundError):
            raise error
    for directory, _, files in os.walk(RUNTIME, followlinks=False, onerror=walk_error):
        for name in files:
            try:
                path = Path(directory) / name
                if not path.is_symlink():
                    disk += path.stat().st_size
            except FileNotFoundError:
                pass
    free = shutil.disk_usage(ROOT).free
    rows = subprocess.check_output(['ps', '-axo', 'rss,command'], text=True).splitlines()
    rss = sum(int(row.split(None, 1)[0]) * 1024 for row in rows if str(RUNTIME) in row and row.strip().split(None, 1)[0].isdigit())
    value = {'at': stamp(), 'test_disk_bytes': disk, 'free_disk_bytes': free, 'owned_rss_bytes': rss,
             'limits': {'test_disk_bytes': 8 * 1024**3, 'min_free_disk_bytes': 20 * 1024**3, 'owned_rss_bytes': 8 * 1024**3}}
    if disk > 8 * 1024**3 or free < 20 * 1024**3 or rss > 8 * 1024**3:
        raise RuntimeError('M4-03B safety budget exceeded: ' + json.dumps(value))
    return value


def marker(directory, port):
    value = json.loads((directory / 'instance.json').read_text())
    if value.get('project') != str(ROOT) or value.get('data') != str(directory / 'data') or value.get('port') != port:
        raise RuntimeError('Private runtime ownership mismatch: ' + directory.name)
    return value


def process_command(pid):
    return subprocess.check_output(['ps', '-p', str(pid), '-o', 'command='], text=True).strip()


def get(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def mysql_runtime():
    mysql_process.R, mysql_process.STATE, mysql_process.ADMIN = MYSQL, MYSQL / 'instance.json', MYSQL / 'admin.cnf'
    mysql_process.PORT, mysql_process.OUT = MYSQL_PORT, OUT / 'environment/mysql-runtime'
    return mysql_process


def sql(statement, timeout=30):
    m = mysql_runtime()
    value = m.state()
    if m.sql('SELECT @@server_uuid,@@port,@@datadir;', 3).strip().split() != [value['server_uuid'], str(MYSQL_PORT), str(MYSQL / 'data') + '/']:
        raise RuntimeError('Private MySQL live identity differs')
    if not m.owned(value.get('pid')):
        raise RuntimeError('Private MySQL process not owned')
    return m.sql(statement, timeout)


def ck_query(statement):
    request = urllib.request.Request(CK_URL + '/?wait_end_of_query=1&log_queries=0', data=statement.encode())
    with urllib.request.urlopen(request, timeout=5) as response:
        return response.read().decode()


def kafka_env():
    env = os.environ.copy()
    env.update(JAVA_HOME=str(distribution_kafka.JAVA), KAFKA_HEAP_OPTS='-Xms128m -Xmx512m', LOG_DIR=str(KAFKA / 'logs'))
    return env


def kafka_command(name, args):
    return subprocess.run([str(distribution_kafka.KAFKA / 'bin' / name)] + args,
                          cwd=ROOT, env=kafka_env(), capture_output=True, text=True, check=True, timeout=30)


def init_marker(directory, port, extra=None):
    if (directory / 'instance.json').exists():
        return marker(directory, port)
    if (directory / 'data').exists():
        raise RuntimeError('Existing private data has no owner marker')
    directory.mkdir(parents=True, exist_ok=True)
    value = {'project': str(ROOT), 'data': str(directory / 'data'), 'port': port, 'created_at': stamp(), 'pid': None}
    value.update(extra or {})
    private_file(directory / 'instance.json', json.dumps(value, indent=2))
    return value


def es_start():
    dist = distribution_es.RUNTIME / distribution_es.PACKAGES['es'][0]
    expected = distribution_es.PACKAGES['es'][3]
    if (dist / '.ticketpulse-package').read_text().strip() != expected:
        raise RuntimeError('Pinned ES release marker differs')
    value = init_marker(ES, ES_PORT)
    if occupied(ES_PORT):
        verify_es()
        return
    for name in ['config', 'config/jvm.options.d', 'data', 'logs', 'tmp']:
        (ES / name).mkdir(exist_ok=True, parents=True)
    for name in ['jvm.options', 'log4j2.properties']:
        content = (dist / 'config' / name).read_text()
        for before, after in [('HeapDumpPath=data', 'HeapDumpPath=' + str(ES / 'data')),
                              ('ErrorFile=logs/', 'ErrorFile=' + str(ES / 'logs') + '/'),
                              ('file=logs/gc.log', 'file=' + str(ES / 'logs/gc.log')),
                              ('ReplayDataFile=logs/', 'ReplayDataFile=' + str(ES / 'logs') + '/')]:
            content = content.replace(before, after)
        (ES / 'config' / name).write_text(content)
    content = chr(10).join(['cluster.name: ticketpulse-m403b', 'node.name: ticketpulse-m403b',
               'network.host: 127.0.0.1', 'http.port: ' + str(ES_PORT), 'transport.port: ' + str(ES_TRANSPORT),
               'discovery.type: single-node', 'xpack.security.enabled: false', 'xpack.ml.enabled: false',
               'path.data: ' + str(ES / 'data'), 'path.logs: ' + str(ES / 'logs'), ''])
    (ES / 'config/elasticsearch.yml').write_text(content)
    env = os.environ.copy()
    env.update(ES_PATH_CONF=str(ES / 'config'), ES_TMPDIR=str(ES / 'tmp'), ES_JAVA_OPTS='-Xms512m -Xmx512m')
    with (ES / 'launch.log').open('a') as log:
        child = subprocess.Popen([str(dist / 'bin/elasticsearch')], cwd=ROOT, env=env, stdout=log,
                                 stderr=subprocess.STDOUT, start_new_session=True)
    value.update(pid=child.pid, started_at=stamp())
    private_file(ES / 'instance.json', json.dumps(value, indent=2))
    for _ in range(250):
        if child.poll() is not None:
            raise RuntimeError('Private ES exited')
        try:
            if get(ES_URL + '/_cluster/health?wait_for_status=yellow&timeout=1s')['status'] in ['yellow', 'green']:
                break
        except OSError:
            pass
        time.sleep(.2)
    else:
        raise RuntimeError('Private ES startup deadline')
    node = next(iter(get(ES_URL + '/_nodes/process,settings')['nodes'].values()))
    value.update(server_pid=node['process']['id'])
    private_file(ES / 'instance.json', json.dumps(value, indent=2))
    verify_es()


def verify_es():
    value = marker(ES, ES_PORT)
    nodes = get(ES_URL + '/_nodes/process,settings')['nodes']
    if len(nodes) != 1:
        raise RuntimeError('Private ES must have exactly one node')
    node = next(iter(nodes.values()))
    command = process_command(node['process']['id'])
    if node['settings']['path']['data'] != str(ES / 'data') or '-Djava.io.tmpdir=' + str(ES / 'tmp') not in command:
        raise RuntimeError('Private ES live process/path differs')
    if node['settings']['cluster']['name'] != 'ticketpulse-m403b':
        raise RuntimeError('Private ES cluster differs')
    return {'cluster': node['settings']['cluster']['name'], 'data': node['settings']['path']['data'],
            'pid': node['process']['id'], 'url': ES_URL, 'version': node['version']}


def ck_start():
    binary = distribution_ck.verify_binary()
    value = init_marker(CK, CK_PORT, {'native_port': CK_NATIVE, 'verified_distribution': binary})
    if occupied(CK_PORT):
        verify_ck()
        return
    distribution_ck.RUNTIME, distribution_ck.CONFIG = CK, CK / 'config.xml'
    content = distribution_ck.config_text().replace('<http_port>18124</http_port>', '<http_port>' + str(CK_PORT) + '</http_port>')
    content = content.replace('<tcp_port>19002</tcp_port>', '<tcp_port>' + str(CK_NATIVE) + '</tcp_port>')
    for name in ['data', 'logs', 'tmp', 'user-files', 'format-schemas']:
        (CK / name).mkdir(exist_ok=True)
    (CK / 'config.xml').write_text(content)
    with (CK / 'launch.log').open('a') as log:
        child = subprocess.Popen([str(distribution_ck.BINARY), 'server', '--config-file=' + str(CK / 'config.xml')],
                                 cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    value.update(pid=child.pid, started_at=stamp())
    private_file(CK / 'instance.json', json.dumps(value, indent=2))
    for _ in range(200):
        if child.poll() is not None:
            raise RuntimeError('Private CK exited')
        try:
            if ck_query('SELECT 1').strip() == '1':
                break
        except OSError:
            pass
        time.sleep(.2)
    else:
        raise RuntimeError('Private CK startup deadline')
    verify_ck()


def verify_ck():
    value = marker(CK, CK_PORT)
    distribution_ck.CONFIG = CK / 'config.xml'
    pid = distribution_ck.server_pid(value['pid'])
    if not pid or '--config-file=' + str(CK / 'config.xml') not in process_command(pid):
        raise RuntimeError('Private CK process/config differs')
    if ck_query('SELECT 1').strip() != '1':
        raise RuntimeError('Private CK not healthy')
    return {'pid': pid, 'url': CK_URL, 'config_sha256': sha(CK / 'config.xml'),
            'memory_tracker_bytes': 4 * 1024**3, 'physical_fault_domains': 1}


def kafka_start():
    for name, _, algorithm, expected, _ in distribution_kafka.PACKAGES:
        if sha(distribution_kafka.RUNTIME / name, algorithm) != expected:
            raise RuntimeError('Pinned Kafka/JDK archive differs')
    value = init_marker(KAFKA, KAFKA_PORT, {'controller_port': CONTROLLER_PORT})
    if occupied(KAFKA_PORT):
        verify_kafka()
        return
    for name in ['data', 'logs']:
        (KAFKA / name).mkdir(exist_ok=True)
    content = chr(10).join(['process.roles=broker,controller', 'node.id=4031',
          'controller.quorum.bootstrap.servers=127.0.0.1:' + str(CONTROLLER_PORT),
          'listeners=PLAINTEXT://127.0.0.1:' + str(KAFKA_PORT) + ',CONTROLLER://127.0.0.1:' + str(CONTROLLER_PORT),
          'advertised.listeners=PLAINTEXT://127.0.0.1:' + str(KAFKA_PORT),
          'controller.listener.names=CONTROLLER', 'listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
          'inter.broker.listener.name=PLAINTEXT', 'log.dirs=' + str(KAFKA / 'data'),
          'num.partitions=3', 'default.replication.factor=1', 'offsets.topic.replication.factor=1',
          'offsets.topic.num.partitions=3', 'transaction.state.log.replication.factor=1',
          'transaction.state.log.min.isr=1', 'min.insync.replicas=1', 'group.initial.rebalance.delay.ms=0',
          'num.network.threads=2', 'num.io.threads=4', 'log.retention.ms=-1', 'log.retention.bytes=-1',
          'auto.create.topics.enable=false', 'unclean.leader.election.enable=false', 'delete.topic.enable=true', ''])
    (KAFKA / 'server.properties').write_text(content)
    if not value.get('cluster_id'):
        value['cluster_id'] = kafka_command('kafka-storage.sh', ['random-uuid']).stdout.strip()
        private_file(KAFKA / 'instance.json', json.dumps(value, indent=2))
    if not (KAFKA / 'data/meta.properties').exists():
        kafka_command('kafka-storage.sh', ['format', '--standalone', '-t', value['cluster_id'], '-c', str(KAFKA / 'server.properties')])
    with (KAFKA / 'launch.log').open('a') as log:
        child = subprocess.Popen([str(distribution_kafka.KAFKA / 'bin/kafka-server-start.sh'), str(KAFKA / 'server.properties')],
                                  cwd=ROOT, env=kafka_env(), stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    value.update(pid=child.pid, started_at=stamp())
    private_file(KAFKA / 'instance.json', json.dumps(value, indent=2))
    for _ in range(200):
        if child.poll() is not None:
            raise RuntimeError('Private Kafka exited')
        if occupied(KAFKA_PORT):
            try:
                verify_kafka()
                return
            except (OSError, RuntimeError, subprocess.CalledProcessError):
                pass
        time.sleep(.2)
    raise RuntimeError('Private Kafka startup deadline')


def verify_kafka():
    value = marker(KAFKA, KAFKA_PORT)
    command = process_command(value['pid'])
    if 'kafka.Kafka' not in command or str(KAFKA / 'server.properties') not in command:
        raise RuntimeError('Private Kafka process differs')
    properties = dict(line.split('=', 1) for line in (KAFKA / 'data/meta.properties').read_text().splitlines()
                      if '=' in line and not line.startswith('#'))
    if properties.get('cluster.id') != value['cluster_id']:
        raise RuntimeError('Private Kafka data identity differs')
    result = kafka_command('kafka-metadata-quorum.sh', ['--bootstrap-server', '127.0.0.1:' + str(KAFKA_PORT), 'describe', '--status']).stdout
    if value['cluster_id'] not in result:
        raise RuntimeError('Private Kafka live cluster identity differs')
    return {'pid': value['pid'], 'cluster_id': value['cluster_id'], 'bootstrap': '127.0.0.1:' + str(KAFKA_PORT),
            'quorum_status': result, 'replication_factor': 1, 'ha_claim': False}


def verify():
    safe_paths()
    m = mysql_runtime()
    v = m.state()
    mysql_live = sql('SELECT @@server_uuid,@@port,@@datadir,@@binlog_format,@@binlog_row_image,@@sync_binlog;').strip()
    return {'at': stamp(), 'mysql': {'uuid': v['server_uuid'], 'port': MYSQL_PORT, 'live': mysql_live, 'pid': v['pid']},
            'es': verify_es(), 'ck': verify_ck(), 'kafka': verify_kafka(), 'budget': budget()}


def environment():
    verify()
    env = os.environ.copy()
    for name in list(env):
        if name.startswith('TICKETPULSE_'):
            del env[name]
    env.update(json.loads(ENV.read_text()))
    return env


def start():
    safe_paths()
    OUT.mkdir(exist_ok=True, parents=True)
    RUNTIME.mkdir(exist_ok=True, parents=True)
    budget()
    for port in [MYSQL_PORT, ES_PORT, ES_TRANSPORT, CK_PORT, CK_NATIVE, KAFKA_PORT, CONTROLLER_PORT]:
        if occupied(port) and not (RUNTIME / 'environment.json').exists():
            raise RuntimeError('Unowned private port is occupied: ' + str(port))
    m = mysql_runtime()
    if not occupied(MYSQL_PORT):
        m.start('baseline')
    es_start()
    ck_start()
    kafka_start()
    proof = verify()
    if ENV.exists():
        value = json.loads(ENV.read_text())
    else:
        password = secrets.token_hex(24)
        # Live ownership of all four processes is confirmed before these grants.
        sql("CREATE USER 'm403b_cdc'@'127.0.0.1' IDENTIFIED BY '" + password + "';"
            "GRANT REPLICATION SLAVE,REPLICATION CLIENT ON *.* TO 'm403b_cdc'@'127.0.0.1';")
        value = {'TICKETPULSE_TEST_DSN': 'root@unix(' + str(MYSQL / 'mysql.sock') + ')/',
                 'TICKETPULSE_TEST_SERVER_UUID': proof['mysql']['uuid'],
                 'TICKETPULSE_CDC_ADDR': '127.0.0.1:' + str(MYSQL_PORT), 'TICKETPULSE_CDC_USER': 'm403b_cdc',
                 'TICKETPULSE_CDC_PASSWORD': password, 'TICKETPULSE_ES_URL': ES_URL, 'TICKETPULSE_CK_URL': CK_URL,
                 'TICKETPULSE_KAFKA_BROKERS': '127.0.0.1:' + str(KAFKA_PORT), 'TICKETPULSE_ISOLATED_SUITE': 'm4-03b',
                 'TICKETPULSE_M4_EVIDENCE_DIR': str(OUT / 'regression/m4'),
                 'TICKETPULSE_M503_EVIDENCE_DIR': str(OUT / 'regression/m503'),
                 'TICKETPULSE_M504_EVIDENCE_DIR': str(OUT / 'regression/m504'),
                 'TICKETPULSE_M403B_EVIDENCE_DIR': str(OUT / 'verification'),
                 'TICKETPULSE_REBUILD_BINARY': str(RUNTIME / 'bin/ticketpulse-rebuild'),
                 'TICKETPULSE_CONTROL_BINARY': str(RUNTIME / 'bin/ticketpulse-control'),
                 'TICKETPULSE_API_BINARY': str(RUNTIME / 'bin/ticketpulse'),
                 'TICKETPULSE_SYNC_BINARY': str(RUNTIME / 'bin/ticketpulse-sync')}
        private_file(ENV, json.dumps(value, indent=2))
    folder = OUT / 'environment' / str(time.time_ns())
    folder.mkdir(parents=True)
    (folder / 'ownership.json').write_text(json.dumps(proof, indent=2) + chr(10))
    for name, path in [('es.yml', ES / 'config/elasticsearch.yml'), ('ck.xml', CK / 'config.xml'), ('kafka.properties', KAFKA / 'server.properties')]:
        (folder / name).write_text(path.read_text())
    print(json.dumps(proof, indent=2))


def stop():
    # Only processes whose marker + effective command point to this run are signalled.
    for directory, port, verify_fn in [(KAFKA, KAFKA_PORT, verify_kafka), (ES, ES_PORT, verify_es), (CK, CK_PORT, verify_ck)]:
        if not occupied(port):
            continue
        proof = verify_fn()
        os.kill(proof['pid'], signal.SIGTERM)
        for _ in range(300):
            if not occupied(port):
                break
            time.sleep(.1)
        else:
            raise RuntimeError('Private process shutdown deadline')
    if occupied(MYSQL_PORT):
        sql('SELECT 1;')
        mysql_runtime().stop()
    print('M4-03B owned processes stopped; all data/evidence retained')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['start', 'verify', 'stop'])
    args = parser.parse_args()
    if args.action == 'start':
        start()
    elif args.action == 'stop':
        stop()
    else:
        print(json.dumps(verify(), indent=2))
