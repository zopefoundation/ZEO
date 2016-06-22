"""SSL configuration support
"""
import os

default_cert_authenticate = 'SIGNED', '-'
def ssl_config(section, server):
    import ssl

    context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    if section.certificate:
        context.load_cert_chain(section.certificate,
                                keyfile=section.key)
    auth = section.authenticate
    if auth:
        if auth in default_cert_authenticate:
            context.load_default_certs(
                ssl.Purpose.CLIENT_AUTH if server else ssl.Purpose.SERVER_AUTH)
        elif os.path.isdir(auth):
            context.load_verify_locations(capath=auth)
        else:
            context.load_verify_locations(cafile=auth)

        context.verify_mode = ssl.CERT_REQUIRED
    else:
        context.verify_mode = ssl.CERT_NONE

    if server:
        context.check_hostname = False
        return context

    context.check_hostname = section.check_hostname or section.server_hostname

    return context, section.server_hostname

def server_ssl(section):
    return ssl_config(section, True)

def client_ssl(section):
    return ssl_config(section, False)

class ClientStorageConfig:

    def __init__(self, config):
        self.config = config
        self.name = config.getSectionName()

    def open(self):
        from ZEO.ClientStorage import ClientStorage
        # config.server is a multikey of socket-connection-address values
        # where the value is a socket family, address tuple.
        config = self.config

        addresses = [server.address for server in config.server]
        options = {}
        if config.blob_cache_size is not None:
            options['blob_cache_size'] = config.blob_cache_size
        if config.blob_cache_size_check is not None:
            options['blob_cache_size_check'] = config.blob_cache_size_check
        if config.client_label is not None:
            options['client_label'] = config.client_label

        ssl = config.ssl
        if ssl:
            options['ssl'] = ssl[0]
            options['ssl_server_hostname'] = ssl[1]

        return ClientStorage(
            addresses,
            blob_dir=config.blob_dir,
            shared_blob_dir=config.shared_blob_dir,
            storage=config.storage,
            cache_size=config.cache_size,
            cache=config.cache_path,
            name=config.name,
            read_only=config.read_only,
            read_only_fallback=config.read_only_fallback,
            wait_timeout=config.wait_timeout,
            **options)
