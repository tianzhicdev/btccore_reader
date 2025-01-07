import hashlib

# Base58 encoding function
def base58_encode(data):
    alphabet = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
    num = int.from_bytes(data, 'big')
    encode = ''
    while num > 0:
        num, rem = divmod(num, 58)
        encode = alphabet[rem] + encode
    pad = 0
    for byte in data:
        if byte == 0:
            pad += 1
        else:
            break
    return '1' * pad + encode

def public_key_to_address(public_key_hex):
    # Step 1: Decode public key from hex
    public_key = bytes.fromhex(public_key_hex)

    # Step 2: SHA-256 hashing
    sha256_hash = hashlib.sha256(public_key).digest()

    # Step 3: RIPEMD-160 hashing
    ripemd160_hash = hashlib.new('ripemd160', sha256_hash).digest()

    # Step 4: Add network byte (0x00 for mainnet)
    network_byte = b'\x00' + ripemd160_hash

    # Step 5: Calculate checksum (SHA-256 twice)
    checksum = hashlib.sha256(hashlib.sha256(network_byte).digest()).digest()[:4]

    # Step 6: Append checksum to network byte + RIPEMD-160 hash
    binary_address = network_byte + checksum

    # Step 7: Convert to Base58
    return base58_encode(binary_address)

# Provided public key
public_key_hex = "04a35ace7cf07a85755906e79daa28f42f22802ee967c5ba768d6b2630a93d34eed964d2da9a5a9cae802d6e0d3a60422e0fa0bb20eba1a7e2aab54879571c8531"

# Calculate Bitcoin address
address = public_key_to_address(public_key_hex)
print(address)  # Output: 1DwUV4pCgukwNbdLSNkkgyFoyDxLcSFC2K

