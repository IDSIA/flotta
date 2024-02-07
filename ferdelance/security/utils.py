from base64 import b64encode, b64decode


def encode_to_transfer(text: str | bytes, encoding: str = "utf8") -> str:
    """Encode a string that will be sent through a transfer between client and server.

    :param text:
        Text to decode.
    :param encoding:
        Encoding to use in the string-byte conversion.
    :return:
        Encoded text.
    """
    if isinstance(text, str):
        text = text.encode(encoding)
    b64_bytes: bytes = b64encode(text)
    out_text: str = b64_bytes.decode(encoding)
    return out_text


def decode_from_transfer(text: str, encoding: str = "utf8") -> str:
    """Decode the string received through a transfer between client and server.

    :param text:
        Text to decode.
    :param encoding:
        Encoding to use in the string-byte conversion.
    :return:
        Decoded bytes.
    """
    in_bytes: bytes = text.encode(encoding)
    b64_bytes: bytes = b64decode(in_bytes)
    out_text: str = b64_bytes.decode(encoding)
    return out_text
