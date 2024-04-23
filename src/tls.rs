use std::{
    fs::File,
    io::{self, BufReader},
    path::Path,
    sync::Arc,
};

use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    server::{danger::ClientCertVerifier, NoClientAuth, WebPkiClientVerifier},
    RootCertStore,
};
use rustls_pemfile::certs;

use crate::server_config::TlsConfig;

fn load_root_cert_store(path: &Path) -> io::Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();
    let ca_certs = load_certs(path)?;
    for cert in ca_certs {
        root_store
            .add(cert)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Failed to load CA certs"))?;
    }
    Ok(root_store)
}

fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    certs(&mut BufReader::new(File::open(path)?)).collect()
}

fn load_private_key<P: AsRef<Path>>(filename: P) -> io::Result<PrivateKeyDer<'static>> {
    let keyfile = File::open(filename.as_ref()).expect("cannot open private key file");
    let mut reader = BufReader::new(keyfile);

    loop {
        match rustls_pemfile::read_one(&mut reader).expect("cannot parse private key .pem file") {
            Some(rustls_pemfile::Item::Pkcs1Key(key)) => return Ok(key.into()),
            Some(rustls_pemfile::Item::Pkcs8Key(key)) => return Ok(key.into()),
            Some(rustls_pemfile::Item::Sec1Key(key)) => return Ok(key.into()),
            None => break,
            _ => {}
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "No valid private key found in file",
    ))
}

pub fn build_mtls_config(tls_config: &TlsConfig) -> Result<Arc<rustls::ServerConfig>, io::Error> {
    let TlsConfig {
        api: _,
        cert_file,
        key_file,
        ca_file,
    } = tls_config;

    let cert_file = TlsConfig::resolve_path(cert_file);
    let key_file = TlsConfig::resolve_path(key_file);
    let ca_file = ca_file
        .as_ref()
        .map(|ca_file| TlsConfig::resolve_path(ca_file));

    let certs = load_certs(&cert_file)?;
    let key = load_private_key(&key_file)?;

    let client_auth: Arc<dyn ClientCertVerifier> = if ca_file.is_some() {
        let root_store = load_root_cert_store(&ca_file.unwrap())?;
        WebPkiClientVerifier::builder(root_store.into())
            .build()
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Error building the client auth verifier{}", err),
                )
            })?
    } else {
        Arc::new(NoClientAuth)
    };

    let config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(client_auth)
        .with_single_cert(certs, key)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

    Ok(Arc::new(config))
}
