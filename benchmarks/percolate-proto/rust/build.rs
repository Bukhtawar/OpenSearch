fn main() {
    if let Ok(dir) = std::env::var("VS_LIB_DIR") {
        println!("cargo:rustc-link-search=native={dir}");
        println!("cargo:rustc-link-lib=static=hs");
        println!("cargo:rustc-link-lib=dylib=stdc++");
        println!("cargo:rustc-cfg=feature=\"vs\"");
    }
    println!("cargo:rerun-if-env-changed=VS_LIB_DIR");
}
