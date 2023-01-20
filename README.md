redis-go-to-master
==================

Small command-line utility (based on the ideas from <https://github.com/flant/redis-sentinel-proxy>) that:

* Is given lists of Redis hosts and ports, keeps checking it for the current master on each port

* Proxies all tcp requests that it receives on each port to the corresponding master for that port

Usage
-----

Create a config file in YAML:

    ports:
      - 6379
    nodes:
      - redis1
      - redis2
    # auth: "Your-Redis-Auth-Key"
    # proxy_connection_timeout: 3

Run redis-go-to-master:
`./redis-go-to-master /path/to/config.yaml`
