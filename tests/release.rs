use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[test]
fn package_script_matches_version_and_reproduces_archive() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let script = root.join("scripts/package-release.sh");
    assert_eq!(
        fs::metadata(&script).unwrap().permissions().mode() & 0o777,
        0o755
    );
    let binary = std::path::PathBuf::from(env!("CARGO_BIN_EXE_agentd-hub"));
    let outputs = tempfile::tempdir().unwrap();
    let first = outputs.path().join("a");
    let second = outputs.path().join("b");
    for output in [&first, &second] {
        let status = Command::new(&script)
            .args([
                "--binary",
                binary.to_str().unwrap(),
                "--output-dir",
                output.to_str().unwrap(),
                "--source-date-epoch",
                "1700000000",
            ])
            .status()
            .unwrap();
        assert!(status.success());
    }
    let first_archive = fs::read_dir(&first)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|extension| extension == "gz"))
        .unwrap();
    let second_archive = fs::read_dir(&second)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|extension| extension == "gz"))
        .unwrap();
    assert_eq!(first_archive.file_name(), second_archive.file_name());
    assert!(
        first_archive
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with(&format!("agentd-hub-{VERSION}-"))
    );
    assert_eq!(
        fs::read(first_archive).unwrap(),
        fs::read(second_archive).unwrap()
    );
    let sums = fs::read_to_string(first.join("SHA256SUMS")).unwrap();
    assert!(sums.contains(&format!("agentd-hub-{VERSION}-")));
}

#[test]
fn release_workflow_is_tag_triggered_and_ci_owned() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let workflow = fs::read_to_string(root.join(".github/workflows/release.yml")).unwrap();
    assert!(workflow.contains("tags: ['v*']"));
    assert!(workflow.contains("cargo fmt --all -- --check"));
    assert!(workflow.contains("cargo clippy --all-targets --all-features -- -D warnings"));
    assert!(workflow.contains("cargo test --locked --all-targets"));
    assert!(workflow.contains("cargo build --release --locked"));
    assert!(workflow.contains("cmp target/release-assets/*.tar.gz"));
    assert!(workflow.contains("gh release create"));
}
