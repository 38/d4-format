use bindgen::Builder as BG;
use std::ops::Not;
use std::path::{Path, PathBuf};

use std::env;
use std::process::Command;

fn create_hts_bindings(includes: &[PathBuf], version: &str, system: bool) -> Result<(), ()> {
    let mut include_params: Vec<_> = includes
        .iter()
        .map(|x| format!("-I{}/htslib", x.to_str().unwrap()))
        .collect();
    if system {
        include_params.push("-DUSE_SYSTEM_HTSLIB".to_string());
    }
    if version != "1.11" || !Path::new("generated/hts.rs").exists() {
        BG::default()
            .header("hts_inc.h")
            .clang_args(&include_params)
            .layout_tests(false)
            .generate_comments(false)
            .generate_inline_functions(false)
            .generate()?
            .write_to_file("generated/hts.rs")
            .expect("Unable to write the generated file");
    }
    Ok(())
}
fn build_own_htslib() -> (Vec<PathBuf>, String, bool) {
    let mut hts_root = PathBuf::from(env::var("OUT_DIR").unwrap());
    hts_root.push("htslib");
    let version = env::var("HTSLIB").map_or_else(|_| "1.11".to_string(), |v| v);

    assert!(Command::new("bash")
        .args(["build_htslib.sh", &version])
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("Unable to build htslib")
        .wait()
        .unwrap()
        .success());

    println!("cargo:rerun-if-changed=build_htslib.sh");
    println!("cargo:rerun-if-changed=hts_inc.h");

    println!("cargo:rustc-link-search={}", hts_root.to_str().unwrap());
    println!("cargo:rustc-link-lib=static=hts");
    //println!("cargo:rustc-link-search=/usr/lib/x86_64-linux-gnu/");
    println!("cargo:rustc-link-lib=static=z");
    println!("cargo:rustc-link-lib=static=bz2");

    (vec![hts_root], version, false)
}
fn main() -> Result<(), std::io::Error> {
    let link_system_lib = env::var("HTSLIB").map_or(false, |htslib| htslib == "system");

    let (htslib_includes, lib_version, system) = if link_system_lib {
        pkg_config::Config::new()
            .atleast_version("1.6")
            .probe("htslib")
            .map_or_else(
                |_| build_own_htslib(),
                |lib| (lib.include_paths, lib.version, true),
            )
    } else {
        build_own_htslib()
    };

    if create_hts_bindings(&htslib_includes, &lib_version, system).is_err() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Bindgen failed",
        ));
    }

    println!("cargo:rerun-if-env-changed=HTSLIB");
    if ["1.6", "1.7", "1.8", "1.9"]
        .iter()
        .any(|&x| lib_version.starts_with(x))
        .not()
    {
        println!("cargo:rustc-cfg=no_bam_hdr_destroy");
    }

    Ok(())
}
