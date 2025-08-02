use std::fs;
use yaml_rust2::YamlLoader;

pub mod config;

fn main() {
    let source = fs::read_to_string("examples/simple/project.yml").unwrap();
    let project = &YamlLoader::load_from_str(&source).unwrap()[0];
    let project = config::parse_project_config(project);

    println!("{project:#?}");

    let source = fs::read_to_string("examples/simple/adapters/logs.yml").unwrap();
    let adapter = &YamlLoader::load_from_str(&source).unwrap()[0];
    let adapter = config::parse_adapter_config(adapter);
    println!("{adapter:#?}");
}
