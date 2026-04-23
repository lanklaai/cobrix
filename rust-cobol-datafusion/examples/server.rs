use async_trait::async_trait;
use cobrix_rust_datafusion::BackendConfig;
use cobrix_rust_datafusion::register_cobol_table;
use datafusion::prelude::SessionContext;
use datafusion_postgres::datafusion_pg_catalog::pg_catalog::context::PgCatalogContextProvider;
use datafusion_postgres::datafusion_pg_catalog::pg_catalog::context::Role;
use datafusion_postgres::datafusion_pg_catalog::setup_pg_catalog;
use datafusion_postgres::{ServerOptions, serve};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create datafusion SessionContext
    let session_context = Arc::new(SessionContext::new());

    let copybook = include_str!("../../rust-cobol/data/CUSTMAST.cbl");
    let data = include_bytes!("../../rust-cobol/data/CUSTOMER.ebcdic");

    register_cobol_table(
        &session_context,
        "CUSTOMER",
        copybook,
        &data[..],
        &BackendConfig::default(),
    )
    .expect("table registered");

    let copybook = include_str!("../../rust-cobol/data/TRANSHST.cbl");
    let data = include_bytes!("../../rust-cobol/data/TRANSACTIONS.ebcdic");

    register_cobol_table(
        &session_context,
        "TRANSACTIONS",
        copybook,
        &data[..],
        &BackendConfig::default(),
    )
    .expect("table registered");

    let copybook = include_str!("../../data/test5d_copybook.cob");
    let data = include_bytes!("../../data/test5_data/COMP.DETAILS.SEP30.DATA.dat");

    register_cobol_table(
        &session_context,
        "COMPANY_DETAILS",
        copybook,
        &data[..],
        &BackendConfig::default(),
    )
    .expect("table registered");

    // TODO: This produces duplicate fields in datafusion (ADDRESS)
    let copybook = include_str!("../../data/test18 special_char.cob");
    let data = include_bytes!("../../data/test18 special_char/HIERARCHICAL.DATA.RDW.dat");

    register_cobol_table(
        &session_context,
        "COMPANY_INFO",
        copybook,
        &data[..],
        &BackendConfig::default(),
    )
    .expect("table registered");

    // Optional: setup pg_catalog schema
    setup_pg_catalog(&session_context, "datafusion", Test)?;

    // Start the Postgres compatible server with SSL/TLS
    let server_options = ServerOptions::new()
        .with_host("127.0.0.1".to_string())
        .with_port(5432);
    // Optional: setup tls
    // .with_tls_cert_path(Some("server.crt".to_string()))
    // .with_tls_key_path(Some("server.key".to_string()));

    serve(session_context, &server_options).await.unwrap();
    Ok(())
}

#[derive(Debug, Clone)]
struct Test;

#[async_trait]
impl PgCatalogContextProvider for Test {
    async fn roles(&self) -> Vec<String> {
        vec!["admin".to_string(), "postgres".to_string()]
    }

    async fn role(&self, _name: &str) -> Option<Role> {
        Some(Role {
            name: "admin".to_string(),
            is_superuser: true,
            can_login: true,
            can_create_db: true,
            can_create_role: true,
            can_create_user: true,
            can_replication: true,
            grants: vec![],
            inherited_roles: vec![],
        })
    }
}
