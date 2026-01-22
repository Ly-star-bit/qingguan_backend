def rotl(x, n):
    # Führt eine bitweise Linksrotation für eine 32-Bit-Ganzzahl durch.
    n = n % 32
    return ((x << n) & 0xFFFFFFFF) | ((x >> (32 - n)) & 0xFFFFFFFF)

def P0(X):
    return X ^ rotl(X, 9) ^ rotl(X, 17)

def P1(X):
    return X ^ rotl(X, 15) ^ rotl(X, 23)

def FF(j, X, Y, Z):
    if 0 <= j <= 15:
        return X ^ Y ^ Z
    else:
        return (X & Y) | (X & Z) | (Y & Z)

def GG(j, X, Y, Z):
    if 0 <= j <= 15:
        return X ^ Y ^ Z
    else:
        # Korrigiertes bitweises NOT für vorzeichenlose 32-Bit-Ganzzahlen
        return (X & Y) | ((~X & 0xFFFFFFFF) & Z)

def sm3_hash(e):
    # Erwartet, dass 'e' ein byte-ähnliches Objekt ist
    t = len(e) * 8
    
    # Padding
    message = bytearray(e)
    message.append(0x80)
    while (len(message) % 64) != 56:
        message.append(0x00)
        
    # Längen-Suffix anhängen (64 Bit, Big-Endian)
    message.extend(t.to_bytes(8, 'big'))
    
    c = len(message) // 64  # Anzahl der 512-Bit-Blöcke
    s = [
        0x7380166f, 0x4914b2b9, 0x172442d7, 0xda8a0600,
        0xa96f30bc, 0x163138aa, 0xe38dee4d, 0xb0fb0e4e
    ]

    for d in range(c):
        W = [0] * 68
        M = [0] * 64

        offset = d * 64
        for i in range(16):
            W[i] = int.from_bytes(message[offset + i*4 : offset + i*4 + 4], 'big')
        
        for i in range(16, 68):
            W[i] = P1(W[i - 16] ^ W[i - 9] ^ rotl(W[i - 3], 15)) ^ rotl(W[i - 13], 7) ^ W[i - 6]

        for i in range(64):
            M[i] = W[i] ^ W[i + 4]

        t1 = 0x79cc4519
        t2 = 0x7a879d8a

        A, B, C, D, E, F, G, H = s

        for j in range(64):
            l = t1 if 0 <= j <= 15 else t2
            
            # Korrigierte Additionen, um Modulo 2^32 sicherzustellen
            ss1_sum = (rotl(A, 12) + E + rotl(l, j)) & 0xFFFFFFFF
            SS1 = rotl(ss1_sum, 7)
            
            SS2 = SS1 ^ rotl(A, 12)
            
            tt1_sum = (FF(j, A, B, C) + D + SS2 + M[j]) & 0xFFFFFFFF
            TT1 = tt1_sum
            
            tt2_sum = (GG(j, E, F, G) + H + SS1 + W[j]) & 0xFFFFFFFF
            TT2 = tt2_sum
            
            D = C
            C = rotl(B, 9)
            B = A
            A = TT1
            H = G
            G = rotl(F, 19)
            F = E
            E = P0(TT2)

        s[0] ^= A
        s[1] ^= B
        s[2] ^= C
        s[3] ^= D
        s[4] ^= E
        s[5] ^= F
        s[6] ^= G
        s[7] ^= H

    result = []
    for val in s:
        result.extend(val.to_bytes(4, 'big'))

    return result




def utf8_to_array(s):
    """Convert string to UTF-8 byte list (list of int)"""
    result = []
    i = 0
    while i < len(s):
        cp = ord(s[i])
        if cp <= 0x7F:
            result.append(cp)
        elif cp <= 0x7FF:
            result.append(0xC0 | (cp >> 6))
            result.append(0x80 | (cp & 0x3F))
        elif 0xD800 <= cp <= 0xDFFF:
            # Handle surrogate pair
            if i + 1 < len(s) and 0xDC00 <= ord(s[i+1]) <= 0xDFFF:
                high = cp - 0xD800
                low = ord(s[i+1]) - 0xDC00
                code_point = 0x10000 + (high << 10) + low
                result.extend([
                    0xF0 | (code_point >> 18),
                    0x80 | ((code_point >> 12) & 0x3F),
                    0x80 | ((code_point >> 6) & 0x3F),
                    0x80 | (code_point & 0x3F)
                ])
                i += 1
            else:
                raise ValueError("Invalid UTF-16 surrogate pair")
        elif cp <= 0xFFFF:
            result.append(0xE0 | (cp >> 12))
            result.append(0x80 | ((cp >> 6) & 0x3F))
            result.append(0x80 | (cp & 0x3F))
        else:
            raise ValueError("Code point out of range")
        i += 1
    return result

def array_to_hex(arr):
    return ''.join(f"{b:02x}" for b in arr)



def sm3_decrypt(input_str):
    byte_arr = utf8_to_array(input_str)
    print(byte_arr)
    hash_bytes = sm3_hash(byte_arr)
    print(hash_bytes)
    return array_to_hex(hash_bytes)


# === Test ===
if __name__ == "__main__":
    msg = "admin123"
    print(sm3_decrypt(msg))