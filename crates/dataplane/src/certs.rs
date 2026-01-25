//! Auto-generation of self-signed TLS certificates for local development.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rcgen::{CertificateParams, DnType, KeyPair, SanType};
use tracing::info;

/// Generate self-signed TLS certificates for local development.
/// Returns (cert_path, key_path) pointing to the generated files.
pub fn generate_dev_certificates(proxy_domain: &str) -> Result<(PathBuf, PathBuf)> {
    let cert_dir = std::env::temp_dir().join("indexify-dataplane-certs");
    std::fs::create_dir_all(&cert_dir)
        .context("Failed to create certificate directory")?;

    let cert_path = cert_dir.join("cert.pem");
    let key_path = cert_dir.join("key.pem");

    // Check if certificates already exist and are recent (less than 24 hours old)
    if certificates_are_valid(&cert_path, &key_path) {
        info!(
            cert_path = %cert_path.display(),
            "Using existing dev certificates"
        );
        return Ok((cert_path, key_path));
    }

    info!(
        proxy_domain = %proxy_domain,
        cert_path = %cert_path.display(),
        "Generating self-signed dev certificates"
    );

    // Generate a new key pair
    let key_pair = KeyPair::generate()
        .context("Failed to generate key pair")?;

    // Configure certificate parameters
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, format!("*.{}", proxy_domain));
    params.distinguished_name.push(DnType::OrganizationName, "Indexify Dev");

    // Add Subject Alternative Names for the wildcard domain
    params.subject_alt_names = vec![
        SanType::DnsName(format!("*.{}", proxy_domain).try_into().unwrap()),
        SanType::DnsName(proxy_domain.to_string().try_into().unwrap()),
        SanType::DnsName("localhost".to_string().try_into().unwrap()),
    ];

    // Generate the certificate
    let cert = params.self_signed(&key_pair)
        .context("Failed to generate self-signed certificate")?;

    // Write certificate and key to files
    std::fs::write(&cert_path, cert.pem())
        .context("Failed to write certificate file")?;
    std::fs::write(&key_path, key_pair.serialize_pem())
        .context("Failed to write key file")?;

    info!(
        cert_path = %cert_path.display(),
        key_path = %key_path.display(),
        "Generated self-signed dev certificates"
    );

    Ok((cert_path, key_path))
}

/// Check if certificates exist and are less than 24 hours old.
fn certificates_are_valid(cert_path: &Path, key_path: &Path) -> bool {
    if !cert_path.exists() || !key_path.exists() {
        return false;
    }

    // Check if cert was modified in the last 24 hours
    let Ok(metadata) = std::fs::metadata(cert_path) else {
        return false;
    };

    let Ok(modified) = metadata.modified() else {
        return false;
    };

    let Ok(age) = modified.elapsed() else {
        return false;
    };

    // Valid if less than 24 hours old
    age.as_secs() < 24 * 60 * 60
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_dev_certificates() {
        let (cert_path, key_path) = generate_dev_certificates("sandboxes.local").unwrap();

        assert!(cert_path.exists());
        assert!(key_path.exists());

        // Verify certificate content
        let cert_content = std::fs::read_to_string(&cert_path).unwrap();
        assert!(cert_content.contains("-----BEGIN CERTIFICATE-----"));

        let key_content = std::fs::read_to_string(&key_path).unwrap();
        assert!(key_content.contains("-----BEGIN PRIVATE KEY-----"));
    }

    #[test]
    fn test_certificates_are_reused() {
        let (cert_path1, _) = generate_dev_certificates("sandboxes.local").unwrap();
        let (cert_path2, _) = generate_dev_certificates("sandboxes.local").unwrap();

        // Should reuse existing certificates
        assert_eq!(cert_path1, cert_path2);
    }
}
