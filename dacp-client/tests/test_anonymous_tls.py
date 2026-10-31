from urllib.parse import urlsplit
import pytest
from dacp.source import _flight_client

@pytest.mark.parametrize('authenticated,explicit_ca', [(False,False),(False,True),(True,False),(True,True)])
def test_only_anonymous_without_ca_skips_certificate_verification(monkeypatch, authenticated, explicit_ca):
    for name in ('DACP_CA','DACP_CERT','DACP_KEY','DACP_TOKEN_FILE'):
        monkeypatch.delenv(name, raising=False)
    captured = {}
    def client(address, **kwargs):
        captured.update(kwargs)
        return object()
    monkeypatch.setattr('dacp.source.flight.FlightClient', client)
    _flight_client(urlsplit('dacp://localhost:18861'), tls_root_certs=b'ca' if explicit_ca else None,
                   token='secret' if authenticated else None)
    assert captured['disable_server_verification'] == (not authenticated and not explicit_ca)
    assert captured['tls_root_certs'] == (b'ca' if explicit_ca else None)


def test_environment_token_keeps_certificate_validation(monkeypatch, tmp_path):
    for name in ('DACP_CA','DACP_CERT','DACP_KEY'):
        monkeypatch.delenv(name, raising=False)
    token = tmp_path / 'token'
    token.write_text('secret')
    monkeypatch.setenv('DACP_TOKEN_FILE', str(token))
    captured = {}
    monkeypatch.setattr('dacp.source.flight.FlightClient', lambda address, **kw: captured.update(kw))
    _flight_client(urlsplit('dacp://localhost:18861'))
    assert captured['disable_server_verification'] is False
