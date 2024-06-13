use bindgen::Builder as BG;
use std::path::{Path, PathBuf};

use std::env;
use std::process::Command;

fn create_bindings(lib_root: &Path) -> Result<(), ()> {
    let include_param = format!("-I{}/libBigWig", lib_root.to_str().unwrap());
    eprintln!("{}", include_param);
    if !Path::new("generated/bigwig.rs").exists() {
        BG::default()
            .header("bigwig_inc.h")
            .clang_args(&[include_param.as_str()])
            .layout_tests(false)
            .generate_comments(false)
            .generate()?
            .write_to_file("generated/bigwig.rs")
            .expect("Unable to write the generated file");
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let lib_root = PathBuf::from(env::var("OUT_DIR").unwrap());
    assert!(Command::new("bash")
        .args(["build-lib.sh", lib_root.to_str().unwrap(), "0.4.4"])
        .spawn()
        .expect("Unable to build libBigWig")
        .wait()
        .unwrap()
        .success());

    create_bindings(lib_root.as_ref()).unwrap();

    println!(
        "cargo:rustc-link-search={}/libBigWig",
        lib_root.to_str().unwrap()
    );
    println!("cargo:rustc-link-lib=static=BigWig");
    println!("cargo:rustc-link-lib=static=z");
    println!("cargo:rerun-if-changed=build-lib.sh");

    Ok(())
}
