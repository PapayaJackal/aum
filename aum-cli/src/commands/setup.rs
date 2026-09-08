//! First-run configuration and administrator setup.
use std::io::{IsTerminal, Write};

use anyhow::{Context, bail};
use clap::Args;

const TEMPLATE: &str = "# Environment variables override these settings.\nsearch_backend = \"opensearch\"\n\n[data]\ndir = \"data\"\n\n[server]\nbase_url = \"http://localhost:8000\"\n\n[opensearch]\nurl = \"http://localhost:9200\"\n\n[tika]\nserver_url = \"http://localhost:9998\"\n";

#[derive(Args)]
pub struct SetupArgs {
    /// Administrator username (prompted when omitted).
    #[arg(long)]
    pub admin: Option<String>,
    /// Generate a password instead of prompting; prints it once after creation.
    #[arg(long)]
    pub generate_password: bool,
}

pub async fn run(args: &SetupArgs) -> anyhow::Result<()> {
    let path = std::path::Path::new("aum.toml");
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(mut file) => {
            file.write_all(TEMPLATE.as_bytes())?;
            println!("Created aum.toml.");
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            println!("Keeping existing aum.toml.");
        }
        Err(e) => {
            return Err(e).context("cannot create aum.toml; run setup in a writable directory");
        }
    }
    let config = aum_core::config::load_config()?;
    let pool = aum_core::bootstrap_db(&config).await;
    let auth = aum_core::auth::AuthService::new(pool, &config.auth);
    if auth.list_users().await?.iter().any(|user| user.is_admin) {
        println!("An administrator already exists; credentials unchanged.");
    } else {
        let username = if let Some(name) = &args.admin {
            name.trim().to_owned()
        } else {
            if !std::io::stdin().is_terminal() {
                bail!("use --admin <username> --generate-password for noninteractive setup");
            }
            print!("Administrator username [admin]: ");
            std::io::stdout().flush()?;
            let mut name = String::new();
            std::io::stdin().read_line(&mut name)?;
            if name.trim().is_empty() {
                "admin".to_owned()
            } else {
                name.trim().to_owned()
            }
        };
        if username.is_empty() {
            bail!("administrator username cannot be empty");
        }
        if auth.get_user_by_username(&username).await?.is_some() {
            bail!(
                "user '{username}' already exists; choose another name or use aum user set-admin"
            );
        }
        let password = if args.generate_password {
            aum_core::auth::password::generate_password(20)
        } else {
            if !std::io::stdin().is_terminal() {
                bail!("use --generate-password for noninteractive setup");
            }
            let password = rpassword::prompt_password("Password: ")?;
            if password != rpassword::prompt_password("Confirm password: ")? {
                bail!("passwords do not match");
            }
            password
        };
        auth.create_user(&username, &password, true).await?;
        println!("Administrator '{username}' created.");
        if args.generate_password {
            println!("Generated password: {password}");
        }
    }
    println!(
        "Next: run `aum doctor`, then `aum serve`.\nOpen {}",
        config.server.base_url
    );
    Ok(())
}
