from ferdelance.security.keys import PrivateKey

from tests.utils import random_string


def test_signature():
    try:
        pk = PrivateKey()

        data = random_string(64000)

        signature = pk.sign(data)

        pk.public_key().verify(data, signature)

    except Exception:
        assert False
