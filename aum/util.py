import base64


def encode_base64(input_string):
    output_bytes = base64.urlsafe_b64encode(input_string.encode("utf-8"))
    output_string = output_bytes.decode("ascii")
    return output_string.rstrip("=")


def decode_base64(input_string):
    input_bytes = input_string.encode("ascii")
    input_len = len(input_bytes)
    padding = b"=" * (3 - ((input_len + 3) % 4))

    # Passing altchars here allows decoding both standard and urlsafe base64
    output_bytes = base64.b64decode(input_bytes + padding, altchars=b"-_")
    return output_bytes.decode("utf-8")
