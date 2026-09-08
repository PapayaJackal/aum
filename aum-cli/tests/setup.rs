//! Exercise onboarding through the actual CLI in isolated working directories.
use std::process::{Command, Output};

fn cli(dir: &std::path::Path, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_aum"));
    for (key, _) in std::env::vars().filter(|(key, _)| key.starts_with("AUM_")) {
        command.env_remove(key);
    }
    command.current_dir(dir).args(args).output().unwrap()
}

#[test]
fn setup_is_repeatable_and_preserves_configuration_and_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let first = cli(
        dir.path(),
        &["setup", "--admin", "owner", "--generate-password"],
    );
    assert!(first.status.success(), "{:?}", first);
    assert!(String::from_utf8_lossy(&first.stdout).contains("Generated password:"));
    let config = std::fs::read(dir.path().join("aum.toml")).unwrap();
    let again = cli(
        dir.path(),
        &["setup", "--admin", "different", "--generate-password"],
    );
    assert!(again.status.success(), "{:?}", again);
    assert!(!String::from_utf8_lossy(&again.stdout).contains("Generated password:"));
    assert_eq!(config, std::fs::read(dir.path().join("aum.toml")).unwrap());
    let users = cli(dir.path(), &["user", "list"]);
    let users = String::from_utf8_lossy(&users.stdout);
    assert!(users.contains("owner"));
    assert!(!users.contains("different"));
}

#[test]
fn setup_keeps_custom_config_and_rejects_empty_username() {
    let dir = tempfile::tempdir().unwrap();
    let config = "[data]\ndir = 'custom-data'\n";
    std::fs::write(dir.path().join("aum.toml"), config).unwrap();
    let result = cli(
        dir.path(),
        &["setup", "--admin", " ", "--generate-password"],
    );
    assert!(!result.status.success());
    assert_eq!(
        std::fs::read_to_string(dir.path().join("aum.toml")).unwrap(),
        config
    );
    let result = cli(
        dir.path(),
        &["setup", "--admin", "owner", "--generate-password"],
    );
    assert!(result.status.success(), "{:?}", result);
    assert!(dir.path().join("custom-data/aum.db").exists());
}

#[test]
fn doctor_reports_all_failures_and_returns_nonzero() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("blocked"), "file").unwrap();
    std::fs::write(dir.path().join("aum.toml"), "[data]\ndir = 'blocked'\n[opensearch]\nurl = 'http://127.0.0.1:0'\n[tika]\nserver_url = 'http://127.0.0.1:0'\n").unwrap();
    let result = cli(dir.path(), &["doctor"]);
    assert!(!result.status.success());
    let output = String::from_utf8_lossy(&result.stdout);
    assert!(output.contains("FAIL Data directory"));
    assert!(output.contains("FAIL Search backend"));
    assert!(output.contains("FAIL Tika instance 1"));
    if !cfg!(feature = "bundle-frontend") {
        assert!(output.contains("FAIL Frontend"));
    }
}

fn http_service(status: &str, body: &str) -> (String, std::thread::JoinHandle<()>) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut request = [0; 4096];
        stream.read(&mut request).unwrap();
        stream.write_all(response.as_bytes()).unwrap();
    });
    (url, thread)
}

#[test]
fn doctor_checks_http_status_and_configured_tika_instances() {
    for (status, succeeds) in [("200 OK", true), ("401 Unauthorized", false)] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("frontend/dist")).unwrap();
        std::fs::write(dir.path().join("frontend/dist/index.html"), "test").unwrap();
        let (search, search_thread) = http_service(status, "{}");
        let (tika, tika_thread) = http_service("200 OK", "Apache Tika");
        std::fs::write(dir.path().join("aum.toml"), format!("[opensearch]\nurl = '{search}'\n[tika]\nserver_url = 'http://127.0.0.1:0'\n[[tika.instances]]\nurl = '{tika}'\nconcurrency = 1\n")).unwrap();
        let result = cli(dir.path(), &["doctor"]);
        search_thread.join().unwrap();
        tika_thread.join().unwrap();
        assert_eq!(result.status.success(), succeeds, "{:?}", result);
        let output = String::from_utf8_lossy(&result.stdout);
        assert!(output.contains("PASS Tika instance 1"));
        assert!(output.contains(if succeeds {
            "PASS Search backend"
        } else {
            "FAIL Search backend"
        }));
    }
}
