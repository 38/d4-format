use clap::{ArgMatches, load_yaml, App};
use d4::index::D4IndexCollection;

use crate::utils::AppResult;


fn build_main(args: &ArgMatches) -> AppResult<()> {
    let input_path = args.value_of("FILE").unwrap();
    let mut index_collection = D4IndexCollection::open_for_write(input_path)?;
    if args.is_present("secondary-frame") {
        log::info!("Creating SFI");
        index_collection.create_secondary_frame_index()?;
        log::info!("Finish creating SFI");
    }
    Ok(())
}

fn main_impl(args: ArgMatches) -> AppResult<bool> {
    if let Some(matches) = args.subcommand_matches("build") {
        build_main(matches)?;
        return Ok(true);
    }
    Ok(false)
}

pub fn entry_point(args: Vec<String>) -> AppResult<()> {
    env_logger::init();
    let yaml = load_yaml!("cli.yml");
    let mut app = App::from_yaml(yaml)
        .version(d4::VERSION);

    let matches = app.clone().get_matches_from(args);

    if !main_impl(matches)? {
        app.print_long_help()?;
    }
    Ok(())
}