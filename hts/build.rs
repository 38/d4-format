use std::path::Path;
use std::process::Command;

use bindgen::Builder as BG;

use num_cpus::get as get_num_cpus;

use std::env;

fn create_hts_bindings(base: &str) -> Result<(), ()> {
    let include_param = format!("-I{}/htslib/", base);
    if !Path::new("generated/hts.rs").exists() {
        BG::default()
            .header("hts_inc.h")
            .clang_arg(include_param.as_str())
            .layout_tests(false)
            .generate_comments(false)
            .generate()?
            .write_to_file("generated/hts.rs")
            .expect("Unable to write the generated file");
    }
    Ok(())
}
fn main() -> Result<(), std::io::Error> {
    let base = format!("{}", env::var("CARGO_MANIFEST_DIR").unwrap());
    let hts_bin_path = format!("{}/htslib/libhts.a", base);
    if let Err(_) = create_hts_bindings(base.as_str()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Bindgen failed",
        ));
    }
    if !Path::new(hts_bin_path.as_str()).exists() {
        Command::new("make")
            .arg(format!("-j{}", get_num_cpus()))
            .current_dir(format!("{}/htslib", base))
            .spawn()
            .expect("Unable to call makefile for htslib");
    }

    if env::var("HTSLIB").ok().map_or(true, |x| x != "dynamic") {
        println!("cargo:rustc-link-search={}/htslib/", base);
        println!("cargo:rustc-link-lib=static=hts");
        println!("cargo:rustc-link-search=/usr/lib/x86_64-linux-gnu/");
        println!("cargo:rustc-link-lib=static=curl");
        println!("cargo:rustc-link-lib=static=z");
        println!("cargo:rustc-link-lib=static=lzma");
        println!("cargo:rustc-link-lib=static=bz2");
    } else {
        println!("cargo:rustc-link-search={}/htslib/", base);
        println!("cargo:rustc-link-lib=hts");
    }

    Ok(())
}
