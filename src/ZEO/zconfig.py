"""SSL configuration support
"""
import os
import sys


def ssl_config(section, server):
    import ssl

    cafile = capath = None
    auth = section.authenticate
    if auth:
        if os.path.isdir(auth):
            capath = auth
        elif auth != 'DYNAMIC':
            cafile = auth

    context = ssl.create_default_context(
        ssl.Purpose.CLIENT_AUTH if server else ssl.Purpose.SERVER_AUTH,
        cafile=cafile, capath=capath)

    if not auth:
        assert not server
        context.load_default_certs()

    if section.certificate:
        password = section.password_function
        if password:
            module, name = password.rsplit('.', 1)
            module = __import__(module, globals(), locals(), ['*'], 0)
            password = getattr(module, name)
        context.load_cert_chain(section.certificate, section.key, password)

    context.verify_mode = ssl.CERT_REQUIRED

    context.verify_flags |= ssl.VERIFY_X509_STRICT | (
        context.cert_store_stats()['crl'] and ssl.VERIFY_CRL_CHECK_LEAF)

    if server:
        context.check_hostname = False
        return context

    context.check_hostname = section.check_hostname

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
            server_sync=config.server_sync,
            wait_timeout=config.wait_timeout,
            **options)
