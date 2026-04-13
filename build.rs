use std::path::PathBuf;
use std::process::Command;

/// Try `gcc -print-libgcc-file-name` and return the parent directory.
fn gcc_include_dir() -> Option<PathBuf> {
    let output = Command::new("gcc")
        .arg("-print-libgcc-file-name")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let path = std::str::from_utf8(&output.stdout).ok()?.trim().to_string();
    std::path::Path::new(&path).parent().map(PathBuf::from)
}

/// Try `clang -print-resource-dir` and return `<resource-dir>/include`.
fn clang_include_dir() -> Option<PathBuf> {
    let output = Command::new("clang")
        .arg("-print-resource-dir")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let dir = std::str::from_utf8(&output.stdout).ok()?.trim().to_string();
    Some(PathBuf::from(dir).join("include"))
}

fn main() {
    // Resolve the compiler internal include path (contains stddef.h, etc.) so
    // that bindgen works on any architecture without a hardcoded path in
    // .cargo/config.toml.  Try gcc first, fall back to clang.
    if let Some(dir) = gcc_include_dir().or_else(clang_include_dir) {
        println!(
            "cargo:rustc-env=BINDGEN_EXTRA_CLANG_ARGS=-I{}",
            dir.display()
        );
    }
}
