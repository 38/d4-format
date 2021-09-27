use clap::{load_yaml, App};
use d4::D4FileMerger;

fn main(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches_from(args);
    let inputs: Vec<_> = matches.values_of("input-files").unwrap().collect();
    let output = matches.value_of("output-file").unwrap();
    let mut merger = D4FileMerger::new(output);
    for input in inputs {
        merger = merger.add_input(input);
    }

    merger.merge()?;
    Ok(())
}

pub fn entry_point(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    main(args)
}
