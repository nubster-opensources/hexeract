//! Keeps the feature list of this crate identical to the one of the `hexeract`
//! facade, and a test file behind every feature. A feature added to the facade
//! without a test here fails this guard.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

/// The names declared under `[features]` in a manifest, `default` excluded.
fn declared_features(manifest_path: &Path) -> BTreeSet<String> {
    let manifest = fs::read_to_string(manifest_path)
        .unwrap_or_else(|error| panic!("{} must be readable: {error}", manifest_path.display()));
    let mut in_features = false;
    let mut names = BTreeSet::new();
    for line in manifest.lines().map(str::trim) {
        if line.starts_with('[') {
            in_features = line == "[features]";
            continue;
        }
        if !in_features || line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((name, _)) = line.split_once('=') {
            let name = name.trim();
            if name != "default" {
                names.insert(name.to_owned());
            }
        }
    }
    names
}

fn manifest_directory() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn every_facade_feature_has_a_matching_feature_here() {
    let facade = declared_features(&manifest_directory().join("../hexeract/Cargo.toml"));
    let here = declared_features(&manifest_directory().join("Cargo.toml"));

    assert!(
        !facade.is_empty(),
        "the facade must declare features, the parser read none"
    );
    let missing_here: Vec<_> = facade.difference(&here).collect();
    let unknown_to_facade: Vec<_> = here.difference(&facade).collect();
    assert!(
        missing_here.is_empty(),
        "facade features without a feature in hexeract-umbrella-tests: {missing_here:?}"
    );
    assert!(
        unknown_to_facade.is_empty(),
        "features in hexeract-umbrella-tests unknown to the facade: {unknown_to_facade:?}"
    );
}

#[test]
fn every_feature_has_its_own_test_file() {
    let here = declared_features(&manifest_directory().join("Cargo.toml"));
    let missing: Vec<_> = here
        .iter()
        .filter(|feature| {
            let file = format!("feature_{}.rs", feature.replace('-', "_"));
            !manifest_directory().join("tests").join(file).is_file()
        })
        .collect();
    assert!(
        missing.is_empty(),
        "features without a tests/feature_<name>.rs file: {missing:?}"
    );
}
