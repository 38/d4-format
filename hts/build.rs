use bindgen::Builder as BG;
use std::path::{Path, PathBuf};

use std::env;
use std::process::Command;

fn create_hts_bindings(includes: &Vec<PathBuf>) -> Result<(), ()> {
    let include_params: Vec<_> = includes.into_iter().map(|x| format!("-I{:?}", x)).collect();
    if env::var("UPDATE_HEADER").map_or(false, |update| update == "1")
        || !Path::new("generated/hts.rs").exists()
    {
        BG::default()
            .header("hts_inc.h")
            .clang_args(&include_params)
            .layout_tests(false)
            .generate_comments(false)
            .generate()?
            .write_to_file("generated/hts.rs")
            .expect("Unable to write the generated file");
    }
    Ok(())
}
fn main() -> Result<(), std::io::Error> {
    let dynamic_link = env::var("HTSLIB").map_or(false, |htslib| htslib == "dynamic");
    let htslib_includes = if dynamic_link && env::var("HTSLIB_VERSION").is_err() {
        pkg_config::Config::new()
            .statik(false)
            .probe("htslib")
            .unwrap()
            .include_paths
    } else {
        let mut hts_root = PathBuf::from(env::var("OUT_DIR").unwrap());
        hts_root.push("htslib");

        assert!(Command::new("bash")
            .args(&["build_htslib.sh"])
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("Unable to build htslib")
            .wait()
            .unwrap()
            .success());

        println!("cargo:rerun-if-changed=build_htslib.sh");

        println!("cargo:rustc-link-search={}", hts_root.to_str().unwrap());

        if !dynamic_link {
            println!("cargo:rustc-link-lib=static=hts");
            println!("cargo:rustc-link-search=/usr/lib/x86_64-linux-gnu/");
            if env::var("CARGO_CFG_TARGET_ENV") == Ok("musl".to_string()) {
                println!("cargo:rustc-link-lib=static=z");
                println!("cargo:rustc-link-lib=static=bz2");
            } else {
                println!("cargo:rustc-link-lib=static=z");
                println!("cargo:rustc-link-lib=static=lzma");
                println!("cargo:rustc-link-lib=static=bz2");
            }
        } else {
            println!("cargo:rustc-link-lib=hts");
        }

        vec![hts_root]
    };

    if let Err(_) = create_hts_bindings(&htslib_includes) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Bindgen failed",
        ));
    }

    println!("cargo:rerun-if-env-changed=HTSLIB");
    println!("cargo:rerun-if-env-changed=HTSLIB_VERSION");

    Ok(())
}
