use clap::{load_yaml, App};
use d4::find_tracks_in_file;

fn main(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches_from(args);
    let input = matches.value_of("input-file").unwrap();

    let mut tracks = Vec::new();
    find_tracks_in_file(input, |_| true, &mut tracks)?;

    for track in tracks {
        if track.components().any(|_| true) {
            println!("{}:{}", input, track.to_string_lossy());
        } else {
            println!("{}", input);
        }
    }

    Ok(())
}

pub fn entry_point(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    main(args)
}
