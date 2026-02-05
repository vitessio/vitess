{
    "ui": "true",
    "disable_mlock": "true",
    "listener": [
        {
            "tcp": {
                "address": "$server:$port",
		"tls_disable": 0,
                "tls_cert_file": "$cert",
                "tls_key_file": "$key"
            }
        }
    ],
    "storage": [
        {
            "inmem": {}
        }
    ],
    "default_lease_ttl": "168h",
    "max_lease_ttl": "720h"
}
