#!/usr/bin/env python3
import os
import redis
from redis.sentinel import Sentinel

# Подключение (из окружения пода)
sentinel_hosts = os.getenv("ALFA_LANGFLOW_FLOW_STATE_REDIS_SENTINELS", "agen-test-ai-redis-ycd-1:6021,agen-test-ai-redis-ycd-2:6021,agen-test-ai-redis-ycd-3:6021")
service_name   = os.getenv("ALFA_LANGFLOW_FLOW_STATE_REDIS_SERVICE_NAME", "mymaster")
password       = os.getenv("ALFA_LANGFLOW_FLOW_STATE_REDIS_PASSWORD", "")
username       = os.getenv("ALFA_LANGFLOW_FLOW_STATE_REDIS_USERNAME", "")
db             = int(os.getenv("ALFA_LANGFLOW_FLOW_STATE_REDIS_DB", "0"))
flow_prefix    = os.getenv("ALFA_LANGFLOW_FLOW_STATE_REDIS_KEY_PREFIX", "aiflowteam-integration:agflow")
use_ssl        = os.getenv("ALFA_LANGFLOW_FLOW_STATE_REDIS_USE_SSL", "true").lower() == "true"
ca_cert        = os.getenv("ALFA_LANGFLOW_FLOW_STATE_REDIS_CA_CERT", "")

qname = os.getenv("ALFI_REDIS_QUEUE_NAME", "alfi:run:queue").strip(":")
base  = f"{flow_prefix}:alfi:jobs:queue:{qname}"
pattern = f"{flow_prefix}:alfi:jobs:queued:*"

# Сентинелы
sentinels = []
for h in sentinel_hosts.split(","):
    h = h.strip()
    if not h:
        continue
    host, port = h.split(":") if ":" in h else (h, 6021)
    sentinels.append((host, int(port)))

sentinel_kwargs = {"password": password, "username": username,
                   "socket_timeout": 10, "socket_connect_timeout": 10}
master_kwargs   = {"password": password, "username": username, "db": db,
                   "socket_timeout": 10, "socket_connect_timeout": 10,
                   "decode_responses": True}
if use_ssl:
    sentinel_kwargs["ssl"] = True
    master_kwargs["ssl"]   = True
    if ca_cert and os.path.exists(ca_cert):
        sentinel_kwargs["ssl_ca_certs"] = ca_cert
        sentinel_kwargs["ssl_cert_reqs"] = "none"
        master_kwargs["ssl_ca_certs"]   = ca_cert
        master_kwargs["ssl_cert_reqs"]  = "none"

sentinel = Sentinel(sentinels, sentinel_kwargs=sentinel_kwargs)
r = sentinel.master_for(service_name, **master_kwargs)

# Смотрим ДО
depth     = r.llen(base)
in_flight = r.llen(f"{base}:processing")
keys = list(r.scan_iter(match=pattern, count=1000))

print(f"В очереди (depth):       {depth}")
print(f"В обработке (in-flight): {in_flight}")
print(f"Payload-ключей:          {len(keys)}")

# Удаляем
deleted_payload = 0
if keys:
    deleted_payload = r.delete(*keys)
deleted_lists = r.delete(base, f"{base}:processing")

print(f"Удалено payload-ключей:  {deleted_payload}")
print(f"Удалено списков очереди: {deleted_lists}")

# Проверка
print(f"Осталось в очереди:      {r.llen(base)}")
print(f"Осталось в обработке:    {r.llen(f'{base}:processing')}")
print(f"Осталось payload-ключей: {len(list(r.scan_iter(match=pattern, count=1000)))}")