//! `aum user` subcommands for managing users, invitations, and permissions.

use anyhow::{Context, bail};
use clap::{Args, Subcommand};

use aum_core::auth::AuthService;
use aum_core::config::AumConfig;

#[derive(Args)]
pub struct UserArgs {
    #[command(subcommand)]
    pub command: UserCommand,
}

#[derive(Subcommand)]
pub enum UserCommand {
    /// Create a local user.
    Create {
        /// Username for the new user.
        username: String,
        /// Create as admin.
        #[arg(long)]
        admin: bool,
        /// Generate a secure random password instead of prompting.
        #[arg(long)]
        generate_password: bool,
    },
    /// List all users.
    List,
    /// Delete a user.
    Delete {
        /// Username to delete.
        username: String,
        /// Skip confirmation prompt.
        #[arg(long, short = 'y')]
        yes: bool,
    },
    /// Reset a user's password.
    SetPassword {
        /// Username whose password to change.
        username: String,
        /// Generate a secure random password instead of prompting.
        #[arg(long)]
        generate_password: bool,
    },
    /// Set or revoke admin status for a user.
    SetAdmin {
        /// Username to modify.
        username: String,
        /// Remove admin status instead of granting it.
        #[arg(long)]
        revoke: bool,
    },
    /// Grant a user access to an index.
    Grant {
        /// Username to grant access to.
        username: String,
        /// Name of the index to grant access to.
        index_name: String,
    },
    /// Revoke a user's access to an index.
    Revoke {
        /// Username to revoke access from.
        username: String,
        /// Name of the index to revoke access from.
        index_name: String,
    },
    /// Generate an invitation link for a new user.
    Invite {
        /// Username for the invited user.
        username: String,
        /// Invite as admin.
        #[arg(long)]
        admin: bool,
        /// Invitation expiry in hours.
        #[arg(long, default_value = "48")]
        expires: i64,
    },
    /// Generate a long-lived session token for programmatic access.
    Token {
        /// Username to generate a token for.
        username: String,
        /// Token lifetime in days.
        #[arg(long, default_value = "365")]
        days: i64,
    },
}

pub async fn run(args: &UserArgs, auth: &AuthService, config: &AumConfig) -> anyhow::Result<()> {
    match &args.command {
        UserCommand::Create {
            username,
            admin,
            generate_password,
        } => cmd_create(auth, username, *admin, *generate_password).await,

        UserCommand::List => cmd_list(auth).await,

        UserCommand::Delete { username, yes } => cmd_delete(auth, username, *yes).await,

        UserCommand::SetPassword {
            username,
            generate_password,
        } => cmd_set_password(auth, username, *generate_password).await,

        UserCommand::SetAdmin { username, revoke } => cmd_set_admin(auth, username, *revoke).await,

        UserCommand::Grant {
            username,
            index_name,
        } => cmd_grant(auth, username, index_name).await,

        UserCommand::Revoke {
            username,
            index_name,
        } => cmd_revoke(auth, username, index_name).await,

        UserCommand::Invite {
            username,
            admin,
            expires,
        } => cmd_invite(auth, config, username, *admin, *expires).await,

        UserCommand::Token { username, days } => cmd_token(auth, username, *days).await,
    }
}

// ---------------------------------------------------------------------------
// Subcommand implementations
// ---------------------------------------------------------------------------

async fn cmd_create(
    auth: &AuthService,
    username: &str,
    is_admin: bool,
    generate: bool,
) -> anyhow::Result<()> {
    let password = obtain_password(generate)?;
    auth.create_user(username, &password, is_admin).await?;
    println!("User '{username}' created.");
    Ok(())
}

async fn cmd_list(auth: &AuthService) -> anyhow::Result<()> {
    let users = auth.list_users().await?;
    if users.is_empty() {
        println!("No users.");
        return Ok(());
    }

    println!("{:<24} {:<8}", "USERNAME", "ADMIN");
    println!("{}", "─".repeat(32));
    for user in &users {
        let admin = if user.is_admin { "yes" } else { "no" };
        println!("{:<24} {:<8}", user.username, admin);
    }
    Ok(())
}

async fn cmd_delete(auth: &AuthService, username: &str, yes: bool) -> anyhow::Result<()> {
    if !yes {
        eprint!("Delete user '{username}'? [y/N] ");
        let mut input = String::new();
        std::io::stdin()
            .read_line(&mut input)
            .context("failed to read confirmation")?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    if auth.delete_user(username).await? {
        println!("User '{username}' deleted.");
    } else {
        bail!("user '{username}' not found");
    }
    Ok(())
}

async fn cmd_set_password(
    auth: &AuthService,
    username: &str,
    generate: bool,
) -> anyhow::Result<()> {
    let password = obtain_password(generate)?;
    auth.set_password(username, &password).await?;
    println!("Password updated for '{username}'.");
    Ok(())
}

async fn cmd_set_admin(auth: &AuthService, username: &str, revoke: bool) -> anyhow::Result<()> {
    let is_admin = !revoke;
    if auth.set_admin(username, is_admin).await? {
        if is_admin {
            println!("User '{username}' is now an admin.");
        } else {
            println!("Admin status revoked for '{username}'.");
        }
    } else {
        bail!("user '{username}' not found");
    }
    Ok(())
}

async fn cmd_grant(auth: &AuthService, username: &str, index_name: &str) -> anyhow::Result<()> {
    if auth.grant_permission(username, index_name).await? {
        println!("Granted '{username}' access to index '{index_name}'.");
    } else {
        println!("User '{username}' already has access to '{index_name}'.");
    }
    Ok(())
}

async fn cmd_revoke(auth: &AuthService, username: &str, index_name: &str) -> anyhow::Result<()> {
    if auth.revoke_permission(username, index_name).await? {
        println!("Revoked '{username}' access to index '{index_name}'.");
    } else {
        println!("User '{username}' did not have access to '{index_name}'.");
    }
    Ok(())
}

async fn cmd_invite(
    auth: &AuthService,
    config: &AumConfig,
    username: &str,
    is_admin: bool,
    expires_hours: i64,
) -> anyhow::Result<()> {
    let invitation = auth
        .create_invitation(username, is_admin, expires_hours)
        .await?;
    let base_url = config.server.base_url.trim_end_matches('/');
    println!("Invitation for '{username}' (expires in {expires_hours}h):");
    println!("{base_url}/#/invite?token={}", invitation.token);
    Ok(())
}

async fn cmd_token(auth: &AuthService, username: &str, days: i64) -> anyhow::Result<()> {
    let user = auth
        .get_user_by_username(username)
        .await?
        .ok_or_else(|| anyhow::anyhow!("user '{username}' not found"))?;
    let token = auth.create_session_with_expiry_days(&user, days).await?;
    println!("{token}");
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Either generate a random password and print it, or prompt interactively with confirmation.
fn obtain_password(generate: bool) -> anyhow::Result<String> {
    if generate {
        let pw = aum_core::auth::password::generate_password(20);
        println!("Generated password: {pw}");
        return Ok(pw);
    }
    prompt_password_with_confirm()
}

fn prompt_password_with_confirm() -> anyhow::Result<String> {
    let password = rpassword::prompt_password("Password: ").context("failed to read password")?;
    let confirm =
        rpassword::prompt_password("Confirm password: ").context("failed to read confirmation")?;
    if password != confirm {
        bail!("passwords do not match");
    }
    Ok(password)
}
