#!/usr/bin/env python3
"""Add a tenant-scoped durable CDC subscription to an existing Artemis deployment.

Run on the broker host. Private JSON on stdin supplies tenantId, brokerUsername,
brokerPassword; optional brokerContainer, brokerConfigDirectory, managementUrl.
Existing files are backed up; unrelated XML/settings and users are preserved.
No broker restart, queue deletion, or permission on another tenant is performed.
"""
import base64
import datetime
import json
import pathlib
import re
import subprocess
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET

cfg = json.load(sys.stdin)
tenant = cfg['tenantId']
user = cfg['brokerUsername']
password = cfg['brokerPassword']
container = cfg.get('brokerContainer', 'message-broker')
for value in (tenant, user, container):
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', value):
        raise SystemExit('Unsafe identifier')
if not re.fullmatch(r'[a-f0-9]{64}', password):
    raise SystemExit('Use a random 32-byte hex broker password')
folder = pathlib.Path(cfg.get('brokerConfigDirectory', '/docker/appjava/src/message-broker/configuration'))
address = 'iqhr.cdc.' + tenant
queue = address + '.history'
role = 'cdc-' + tenant.lower()
stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')

def docker_read(name):
    return subprocess.check_output(['docker', 'exec', container, 'cat', '/var/lib/artemis-instance/etc/' + name], text=True)

def save(name, content):
    target = folder / name
    old = target.read_text()
    if old != content:
        backup = target.with_name(name + '.before-cdc-' + stamp)
        backup.write_text(old)
        backup.chmod(0o600)
        # Preserve the inode used by the container's existing bind mount.
        target.write_text(content)
    actual = docker_read(name)
    if actual != content:
        subprocess.run(['docker','exec',container,'cp','/var/lib/artemis-instance/etc/'+name,
                        '/var/lib/artemis-instance/etc/'+name+'.before-cdc-'+stamp],check=True)
        subprocess.run(['docker','exec','-i',container,'sh','-c','cat > /var/lib/artemis-instance/etc/'+name],
                       input=content,text=True,check=True)

def upsert_property(content, key, value):
    pattern = re.compile(r'^' + re.escape(key) + r'=.*$', re.MULTILINE)
    if pattern.search(content):
        old = pattern.search(content).group(0)
        if old != key + '=' + value:
            raise SystemExit('Existing CDC credential/role differs; explicit rotation is required')
        return content
    return content.rstrip() + '\n' + key + '=' + value + '\n'

xml = (folder / 'broker.xml').read_text()
security = f'''            <security-setting match="{address}">
                <permission type="send" roles="{role},amq"/>
                <permission type="consume" roles="{role},amq"/>
                <permission type="browse" roles="{role},amq"/>
                <permission type="manage" roles="amq"/>
            </security-setting>
'''
settings = f'''            <address-setting match="{address}">
                <max-delivery-attempts>-1</max-delivery-attempts>
                <redelivery-delay>1000</redelivery-delay>
                <max-redelivery-delay>30000</max-redelivery-delay>
                <redelivery-delay-multiplier>2.0</redelivery-delay-multiplier>
                <expiry-delay>-1</expiry-delay>
                <min-expiry-delay>-1</min-expiry-delay>
                <max-expiry-delay>-1</max-expiry-delay>
                <address-full-policy>PAGE</address-full-policy>
                <max-size-bytes>16777216</max-size-bytes>
                <page-size-bytes>1048576</page-size-bytes>
                <auto-create-addresses>false</auto-create-addresses>
                <auto-create-queues>false</auto-create-queues>
                <auto-delete-addresses>false</auto-delete-addresses>
                <auto-delete-queues>false</auto-delete-queues>
                <auto-delete-created-queues>false</auto-delete-created-queues>
                <default-exclusive-queue>true</default-exclusive-queue>
                <default-purge-on-no-consumers>false</default-purge-on-no-consumers>
            </address-setting>
'''
topology = f'''            <address name="{address}">
                <multicast><queue name="{queue}"/></multicast>
            </address>
'''
for marker, block, closing in [
    (f'<security-setting match="{address}">', security, '</security-settings>'),
    (f'<address-setting match="{address}">', settings, '</address-settings>'),
    (f'<address name="{address}">', topology, '</addresses>'),
]:
    if marker not in xml:
        if xml.count(closing) != 1:
            raise SystemExit('Unexpected broker configuration shape')
        xml = xml.replace(closing, block + '        ' + closing)
ET.fromstring(xml)
save('artemis-users.properties', upsert_property((folder/'artemis-users.properties').read_text(), user, password))
save('artemis-roles.properties', upsert_property((folder/'artemis-roles.properties').read_text(), role, user))
save('broker.xml', xml)

# Readback uses the existing administrator account, never prints credentials.
users = dict(line.split('=',1) for line in (folder/'artemis-users.properties').read_text().splitlines()
             if line and not line.startswith('#') and '=' in line)
admin = cfg.get('brokerAdminUser', 'admin')
if users[admin].startswith('ENC('):
    raise SystemExit('Config applied; set a supported external management credential for readback')
headers = {'Authorization':'Basic '+base64.b64encode((admin+':'+users[admin]).encode()).decode(),
           'Content-Type':'application/json','Origin':cfg.get('managementOrigin','http://0.0.0.0:8161')}
url = cfg.get('managementUrl','http://127.0.0.1:8161/console/jolokia')
def call(payload):
    request = urllib.request.Request(url,data=json.dumps(payload).encode(),headers=headers)
    with urllib.request.urlopen(request,timeout=15) as response:
        result = json.load(response)
    if result.get('status') != 200:
        raise RuntimeError(result.get('error_type','Management readback failed'))
    return result['value']

pattern = 'org.apache.activemq.artemis:broker=*,component=addresses,address="'+address+'",subcomponent=queues,routing-type="multicast",queue="'+queue+'"'
deadline = time.monotonic() + 35
matches = []
while time.monotonic() < deadline:
    matches = call({'type':'search','mbean':pattern})
    if matches:
        break
    time.sleep(1)
if len(matches) != 1:
    raise SystemExit('Configuration saved but durable queue readback failed; inspect broker logs')
attrs = call({'type':'read','mbean':matches[0], 'attribute':['Durable','Exclusive','PurgeOnNoConsumers','AutoDelete','MessageCount']})
if attrs.get('Durable') is not True or attrs.get('Exclusive') is not True or attrs.get('PurgeOnNoConsumers') is not False or attrs.get('AutoDelete') is not False:
    raise SystemExit('Queue properties do not meet the CDC durability contract')
print(json.dumps({'address':address,'queue':queue,'durability':attrs,'provisioned':True}))
