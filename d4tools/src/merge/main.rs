use clap::{load_yaml, App};
use d4::D4FileMerger;

fn main(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml)
        .version(d4tools::VERSION)
        .get_matches_from(args);
    let inputs: Vec<_> = matches.values_of("input-files").unwrap().collect();
    let output = matches.value_of("output-file").unwrap();
    let mut merger = D4FileMerger::new(output);
    for input in inputs {
        if let Some(split_pos) = input.find(':') {
            let path = &input[..split_pos];
            let tag = &input[split_pos + 1..];
            merger = merger.add_input_with_tag(path, tag);
        } else {
            merger = merger.add_input(input);
        }
    }

    merger.merge()?;
    Ok(())
}

pub fn entry_point(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    main(args)
}
