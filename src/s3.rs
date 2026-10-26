//! S3-compatible object store client (ureq 2) with hand-rolled AWS SigV4.
//! Path-style URLs: {endpoint}/{bucket}/{key}. Conditional PUTs (If-None-Match:*,
//! If-Match:<etag>) are the commit protocol's foundation (see PAPERS.md #1).

use crate::store::{ObjectStore, StoreError, StoreResult};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

fn hmac(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut m = <HmacSha256 as Mac>::new_from_slice(key).expect("hmac accepts any key len");
    m.update(msg);
    m.finalize().into_bytes().to_vec()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fn sha256(data: &[u8]) -> Vec<u8> {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().to_vec()
}

/// Hinnant's civil_from_days: days since 1970-01-01 -> (y, m, d).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// (YYYYMMDD, YYYYMMDDTHHMMSSZ) for a unix timestamp.
fn amz_datetime(unix: u64) -> (String, String) {
    let days = (unix / 86400) as i64;
    let secs = unix % 86400;
    let (y, m, d) = civil_from_days(days);
    let date = format!("{:04}{:02}{:02}", y, m, d);
    let stamp = format!(
        "{}T{:02}{:02}{:02}Z",
        date,
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    );
    (date, stamp)
}

/// Percent-encode per SigV4 (unreserved = A-Za-z0-9-._~ kept, else %XX).
fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        let unres = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~');
        if unres || (b == b'/' && !encode_slash) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

pub struct S3Store {
    agent: ureq::Agent,
    endpoint: String, // e.g. https://s3.amazonaws.com or http://127.0.0.1:9000
    bucket: String,
    prefix: String, // logical keys live under prefix + key
    region: String,
    access_key: String,
    secret_key: String,
}

impl S3Store {
    pub fn new(
        endpoint: &str,
        bucket: &str,
        prefix: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Self {
        let endpoint = endpoint.trim_end_matches('/').to_string();
        let prefix = prefix.trim_matches('/');
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(std::time::Duration::from_secs(10))
            .build();
        S3Store {
            agent,
            endpoint,
            bucket: bucket.to_string(),
            prefix: if prefix.is_empty() { String::new() } else { format!("{}/", prefix) },
            region: region.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
        }
    }

    fn object_url(&self, key: &str) -> String {
        format!(
            "{}/{}/{}{}",
            self.endpoint,
            self.bucket,
            uri_encode(&self.prefix, false),
            uri_encode(key, false)
        )
    }

    fn host_of(url: &str) -> String {
        url.split("://")
            .nth(1)
            .and_then(|rest| rest.split('/').next())
            .unwrap_or("")
            .to_string()
    }

    fn path_of(url: &str) -> String {
        let base = url.split_once('?').map(|(b, _)| b).unwrap_or(url);
        match base.split_once("://") {
            Some((_, rest)) => match rest.find('/') {
                Some(i) => rest[i..].to_string(),
                None => "/".to_string(),
            },
            None => "/".to_string(),
        }
    }

    /// Core signed request. `extra` headers must have lowercase names
    /// (host + x-amz-* + any signed standard header like range/if-match).
    // clippy: ureq::Error is third-party and large; boxing it would ripple
    // through every caller for zero benefit
    #[allow(clippy::result_large_err)]
    fn signed(
        &self,
        method: &str,
        url: &str,
        extra: &[(String, String)],
        query: &[(String, String)],
        body: &[u8],
    ) -> Result<ureq::Response, ureq::Error> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let (date, stamp) = amz_datetime(now);
        let payload_hash = hex(&sha256(body));
        let mut hdrs: Vec<(String, String)> = vec![
            ("host".to_string(), Self::host_of(url)),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), stamp.clone()),
        ];
        for (k, v) in extra {
            hdrs.push((k.to_ascii_lowercase(), v.clone()));
        }
        hdrs.sort();
        let canonical_headers: String =
            hdrs.iter().map(|(k, v)| format!("{}:{}\n", k, v.trim())).collect();
        let signed_headers: String =
            hdrs.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>().join(";");
        let mut q: Vec<(String, String)> = query.to_vec();
        q.sort();
        let canonical_query: String = q
            .iter()
            .map(|(k, v)| format!("{}={}", uri_encode(k, true), uri_encode(v, true)))
            .collect::<Vec<_>>()
            .join("&");
        let path = Self::path_of(url);
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method, path, canonical_query, canonical_headers, signed_headers, payload_hash
        );
        let scope = format!("{}/{}/s3/aws4_request", date, self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            stamp,
            scope,
            hex(&sha256(canonical_request.as_bytes()))
        );
        let k_date = hmac(format!("AWS4{}", self.secret_key).as_bytes(), date.as_bytes());
        let k_region = hmac(&k_date, self.region.as_bytes());
        let k_service = hmac(&k_region, b"s3");
        let k_signing = hmac(&k_service, b"aws4_request");
        let signature = hex(&hmac(&k_signing, string_to_sign.as_bytes()));
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.access_key, scope, signed_headers, signature
        );
        let mut req = self.agent.request(method, url);
        for (k, v) in &hdrs {
            if k != "host" {
                req = req.set(k, v);
            }
        }
        req = req.set("Authorization", &auth);
        req.send_bytes(body)
    }
}

fn body_bytes(resp: ureq::Response) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut r = resp.into_reader();
    let _ = std::io::Read::read_to_end(&mut r, &mut buf);
    buf
}

fn status_of(e: &ureq::Error) -> Option<u16> {
    match e {
        ureq::Error::Status(c, _) => Some(*c),
        _ => None,
    }
}

fn unescape_xml(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

fn map_err(key: &str, e: ureq::Error) -> StoreError {
    match status_of(&e) {
        Some(404) => StoreError::NotFound(key.to_string()),
        Some(412) => StoreError::PreconditionFailed(key.to_string()),
        Some(c) => StoreError::Io(format!("{} -> HTTP {}", key, c)),
        None => StoreError::Io(format!("{} -> {}", key, e)),
    }
}

impl ObjectStore for S3Store {
    fn put(&self, key: &str, data: &[u8]) -> StoreResult<()> {
        let url = self.object_url(key);
        self.signed("PUT", &url, &[], &[], data)
            .map_err(|e| map_err(key, e))?;
        Ok(())
    }

    fn put_if_absent(&self, key: &str, data: &[u8]) -> StoreResult<bool> {
        let url = self.object_url(key);
        match self.signed(
            "PUT",
            &url,
            &[("if-none-match".into(), "*".into())],
            &[],
            data,
        ) {
            Ok(_) => Ok(true),
            Err(e) if status_of(&e) == Some(412) => Ok(false),
            Err(e) => Err(map_err(key, e)),
        }
    }

    fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> StoreResult<bool> {
        let url = self.object_url(key);
        let clean = etag.trim_matches('"');
        let hdr = format!("\"{}\"", clean);
        match self.signed("PUT", &url, &[("if-match".into(), hdr)], &[], data) {
            Ok(_) => Ok(true),
            Err(e) if status_of(&e) == Some(412) => Ok(false),
            // absent object: CAS succeeds only if absence was expected
            Err(e) if status_of(&e) == Some(404) => {
                if clean.is_empty() {
                    self.signed("PUT", &url, &[], &[], data)
                        .map(|_| true)
                        .map_err(|e| map_err(key, e))
                } else {
                    Ok(false)
                }
            }
            Err(e) => Err(map_err(key, e)),
        }
    }

    fn get(&self, key: &str) -> StoreResult<Vec<u8>> {
        let url = self.object_url(key);
        let resp = self
            .signed("GET", &url, &[], &[], &[])
            .map_err(|e| map_err(key, e))?;
        Ok(body_bytes(resp))
    }

    fn head(&self, key: &str) -> StoreResult<(String, u64)> {
        let url = self.object_url(key);
        let resp = self
            .signed("HEAD", &url, &[], &[], &[])
            .map_err(|e| map_err(key, e))?;
        let etag = resp.header("etag").unwrap_or("").trim_matches('"').to_string();
        let len: u64 = resp
            .header("content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        Ok((etag, len))
    }

    fn delete(&self, key: &str) -> StoreResult<()> {
        let url = self.object_url(key);
        self.signed("DELETE", &url, &[], &[], &[])
            .map_err(|e| map_err(key, e))?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> StoreResult<Vec<String>> {
        let full = format!("{}{}", self.prefix, prefix);
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut q: Vec<(String, String)> = vec![
                ("list-type".to_string(), "2".to_string()),
                ("prefix".to_string(), full.clone()),
            ];
            if let Some(t) = &token {
                q.push(("continuation-token".to_string(), t.clone()));
            }
            q.sort();
            let qs = q
                .iter()
                .map(|(k, v)| format!("{}={}", uri_encode(k, true), uri_encode(v, true)))
                .collect::<Vec<_>>()
                .join("&");
            let url = format!("{}/{}?{}", self.endpoint, self.bucket, qs);
            let resp = self
                .signed("GET", &url, &[], &q, &[])
                .map_err(|e| map_err("list", e))?;
            let body = String::from_utf8_lossy(&body_bytes(resp)).into_owned();
            let mut rest = body.as_str();
            while let Some(i) = rest.find("<Key>") {
                let after = &rest[i + 5..];
                match after.find("</Key>") {
                    Some(j) => {
                        keys.push(unescape_xml(&after[..j]));
                        rest = &after[j + 6..];
                    }
                    None => break,
                }
            }
            if body.contains("<IsTruncated>true</IsTruncated>") {
                token = body
                    .split("<NextContinuationToken>")
                    .nth(1)
                    .and_then(|a| a.split("</NextContinuationToken>").next())
                    .map(unescape_xml);
                if token.is_none() {
                    return Err(StoreError::Io("truncated LIST without token".into()));
                }
            } else {
                break;
            }
        }
        let strip = self.prefix.len();
        Ok(keys
            .into_iter()
            .map(|k| k[strip.min(k.len())..].to_string())
            .collect())
    }


    #[cfg(test)]
    fn get_range(&self, key: &str, a: u64, b_inclusive: u64) -> StoreResult<Vec<u8>> {
        let url = self.object_url(key);
        let resp = self
            .signed(
                "GET",
                &url,
                &[("range".to_string(), format!("bytes={}-{}", a, b_inclusive))],
                &[],
                &[],
            )
            .map_err(|e| map_err(key, e))?;
        Ok(body_bytes(resp))
    }
}

/// Raw SigV4 signature (used by the AWS doc-vector test; production
/// requests sign inline in `request` below).
#[cfg(test)]
#[allow(clippy::too_many_arguments)] // mirrors the SigV4 input set verbatim
fn sign(
    method: &str,
    path: &str,
    headers: &[(String, String)],
    _access_key: &str,
    secret_key: &str,
    region: &str,
    service: &str,
    stamp: &str,
    payload_hash: &str,
) -> String {
    let date = &stamp[..8];
    let mut hdrs: Vec<(String, String)> = headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    hdrs.sort();
    let canonical_headers: String =
        hdrs.iter().map(|(k, v)| format!("{}:{}\n", k, v)).collect();
    let signed_headers: String = hdrs.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>().join(";");
    let canonical_request = format!(
        "{}\n{}\n\n{}\n{}\n{}",
        method, path, canonical_headers, signed_headers, payload_hash
    );
    let scope = format!("{}/{}/{}/aws4_request", date, region, service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        stamp,
        scope,
        hex(&sha256(canonical_request.as_bytes()))
    );
    let k_date = hmac(format!("AWS4{}", secret_key).as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    let k_signing = hmac(&k_service, b"aws4_request");
    hex(&hmac(&k_signing, string_to_sign.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AWS SigV4 documentation test vector: GET /test.txt on
    /// example.s3.amazonaws.com, us-east-1, empty payload.
    #[test]
    fn aws_doc_vector() {
        let (date, stamp) = amz_datetime(1_369_353_600); // 2013-05-24T00:00:00Z
        assert_eq!(date, "20130524");
        assert_eq!(stamp, "20130524T000000Z");
        let empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let sig = sign(
            "GET",
            "/test.txt",
            &[
                ("host".to_string(), "examplebucket.s3.amazonaws.com".to_string()),
                ("range".to_string(), "bytes=0-9".to_string()),
                ("x-amz-content-sha256".to_string(), empty.to_string()),
                ("x-amz-date".to_string(), "20130524T000000Z".to_string()),
            ],
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "us-east-1",
            "s3",
            "20130524T000000Z",
            empty,
        );
        assert_eq!(
            sig,
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn uri_encode_cases() {
        assert_eq!(uri_encode("a/b", true), "a%2Fb");
        assert_eq!(uri_encode("a/b", false), "a/b");
        assert_eq!(uri_encode("a b~c", true), "a%20b~c");
        assert_eq!(uri_encode("t/1-0.wal", false), "t/1-0.wal");
    }
}

