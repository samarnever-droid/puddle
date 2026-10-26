//! CRC32C (Castagnoli) — integrity checksums for WAL frames and parts.

const POLY: u32 = 0x82f6_3b78;

fn table() -> &'static [u32; 256] {
    use std::sync::OnceLock;
    static T: OnceLock<[u32; 256]> = OnceLock::new();
    T.get_or_init(|| {
        let mut t = [0u32; 256];
        for i in 0..256u32 {
            let mut c = i;
            for _ in 0..8 {
                c = if c & 1 != 0 { POLY ^ (c >> 1) } else { c >> 1 };
            }
            t[i as usize] = c;
        }
        t
    })
}

pub fn crc32c(data: &[u8]) -> u32 {
    crc32c_update(0, data)
}

pub fn crc32c_update(mut crc: u32, data: &[u8]) -> u32 {
    let t = table();
    crc = !crc;
    for &b in data {
        crc = t[((crc ^ b as u32) & 0xff) as usize] ^ (crc >> 8);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    // Standard CRC32C test vectors
    #[test]
    fn vectors() {
        assert_eq!(crc32c(b""), 0x0000_0000);
        assert_eq!(crc32c(b"a"), 0xc1d0_4330);
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
        // RFC 3720 B.4 vectors
        assert_eq!(crc32c(&[0u8; 32]), 0x8a91_36aa);
        assert_eq!(crc32c(&[0xffu8; 32]), 0x62a8_ab43);
    }
}
