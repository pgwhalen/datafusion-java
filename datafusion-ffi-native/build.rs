use diplomat_tool::config::Config;
use diplomat_tool::DocsUrlGenerator;
use std::collections::HashMap;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let entry = manifest_dir.join("src/lib.rs");
    let config_file = manifest_dir.parent().unwrap().join("diplomat-config.toml");

    // Output to where Gradle expects generated sources
    let out_folder = manifest_dir
        .parent()
        .unwrap()
        .join("datafusion-ffi-java/build/diplomat-generated");

    let mut config = Config::default();
    config
        .read_file(&config_file)
        .expect("Failed to read diplomat config");

    let docs_url_gen = DocsUrlGenerator::with_base_urls(None, HashMap::new());

    diplomat_tool::gen(&entry, "java", &out_folder, &docs_url_gen, config, true)
        .expect("Failed to generate Diplomat Java bindings");

    println!("cargo:rerun-if-changed=src/");
    println!("cargo:rerun-if-changed=../diplomat-config.toml");
}
