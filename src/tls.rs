use std::{
    fs,
    io,
    io::BufReader,
    path::{Path, PathBuf},
    sync::Arc,
    vec::Vec,
};

use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    server::{danger::ClientCertVerifier, NoClientAuth, WebPkiClientVerifier},
};
use tokio_rustls::TlsAcceptor;

use crate::server_config::TlsConfig;

/// Creates a Rustls TLS acceptor, supporting mTLS if a root CA certificate file
/// is provided.
pub async fn build_mtls_acceptor(tls_config: &TlsConfig) -> Result<TlsAcceptor, io::Error> {
    // deconstruct the TlsConfig as cert_file, key_file, ca_file
    let TlsConfig {
        api: _,
        cert_file,
        key_file,
        ca_file,
    } = tls_config;

    let cert_file = TlsConfig::resolve_path(cert_file);
    let key_file = TlsConfig::resolve_path(key_file);

    // Build TLS configuration.
    let mut tls_config = {
        // Load server key pair
        let (certs, key) = load_keypair(&cert_file, &key_file).unwrap();

        let client_auth: Arc<dyn ClientCertVerifier> = if ca_file.is_some() {
            let ca_file = TlsConfig::resolve_path(ca_file.as_ref().unwrap());
            // Load root CA certificate file used to verify client certificate
            let rootstore = Arc::new(load_root_store(ca_file)?);

            WebPkiClientVerifier::builder(
                // allow only certificates signed by a trusted CA
                rootstore,
            )
            .build()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("{}", err)))?
        } else {
            Arc::new(NoClientAuth)
        };

        rustls::ServerConfig::builder()
            .with_client_cert_verifier(client_auth)
            .with_single_cert(certs, key)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("{}", err)))?
    };

    tls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(tls_config)))
}

fn load_keypair(
    certfile: &PathBuf,
    keyfile: &PathBuf,
) -> io::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    // Load and return certificate chain.
    let certs = load_certs(certfile);

    // Load and return a single private key.
    let key = load_private_key(keyfile);

    Ok((certs, key))
}

fn load_certs<P: AsRef<Path>>(filename: P) -> Vec<CertificateDer<'static>> {
    let certfile = fs::File::open(filename).expect("cannot open certificate file");
    let mut reader = BufReader::new(certfile);
    rustls_pemfile::certs(&mut reader)
        .map(|result| result.unwrap())
        .collect()
}

fn load_private_key<P: AsRef<Path>>(filename: P) -> PrivateKeyDer<'static> {
    let keyfile = fs::File::open(filename.as_ref()).expect("cannot open private key file");
    let mut reader = BufReader::new(keyfile);

    loop {
        match rustls_pemfile::read_one(&mut reader).expect("cannot parse private key .pem file") {
            Some(rustls_pemfile::Item::Pkcs1Key(key)) => return key.into(),
            Some(rustls_pemfile::Item::Pkcs8Key(key)) => return key.into(),
            Some(rustls_pemfile::Item::Sec1Key(key)) => return key.into(),
            None => break,
            _ => {}
        }
    }

    panic!(
        "no keys found in {:?} (encrypted keys not supported)",
        filename.as_ref()
    );
}

fn load_root_store<P: AsRef<Path>>(filename: P) -> io::Result<rustls::RootCertStore> {
    let roots = load_certs(filename);
    let mut store = rustls::RootCertStore::empty();
    for root in roots {
        store
            .add(root)
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "cannot add root certificate"))?;
    }

    Ok(store)
}
