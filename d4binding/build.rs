use std::env;
use std::path::Path;
fn main() {
    let binding_dir_str = format!("{}/bindgen", env::var("CARGO_MANIFEST_DIR").unwrap());
    let binding_dir = Path::new(binding_dir_str.as_str());
    if !binding_dir.is_dir() {
        std::fs::create_dir_all(binding_dir).unwrap();
    }

    if env::var("UPDATE_HEADER").map_or(false, |update| update == "1")
        || !Path::new("bindgen/c_api.rs").exists()
    {
        bindgen::Builder::default()
            .header(format!(
                "{}/include/d4.h",
                env::var("CARGO_MANIFEST_DIR").unwrap()
            ))
            .layout_tests(false)
            .generate_comments(false)
            .generate()
            .unwrap()
            .write_to_file("bindgen/c_api.rs")
            .expect("Unable to write the generated file");
    }
}
