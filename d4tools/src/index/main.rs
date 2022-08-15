use clap::{load_yaml, App, ArgMatches};
use d4::index::{D4IndexCollection, Sum};

use d4tools::AppResult;

fn build_main(args: &ArgMatches) -> AppResult<()> {
    let input_path = args.value_of("FILE").unwrap();
    let mut index_collection = D4IndexCollection::open_for_write(input_path)?;
    if args.is_present("secondary-frame") {
        log::info!("Creating SFI");
        index_collection.create_secondary_frame_index()?;
        log::info!("Finish creating SFI");
    }
    if args.is_present("sum") {
        if !args.is_present("secondary-frame") {
            index_collection.create_secondary_frame_index().ok();
        }
        index_collection.create_sum_index()?;
    }
    Ok(())
}

fn show_main(args: &ArgMatches) -> AppResult<()> {
    let input_path = args.value_of("FILE").unwrap();
    let index_collection = D4IndexCollection::from_reader(std::fs::File::open(input_path)?)?;
    match args.value_of("INDEX_TYPE").unwrap().to_lowercase().as_str() {
        "sfi" => {
            let index = index_collection.load_seconary_frame_index()?;
            index.print_secondary_table_index(std::io::stdout())?;
        }
        "sum" => {
            let index = index_collection.load_data_index::<Sum>()?;
            index.print_index();
        }
        _ => {
            panic!("Unsupported index type")
        }
    }
    Ok(())
}

fn main_impl(args: ArgMatches) -> AppResult<bool> {
    if let Some(matches) = args.subcommand_matches("build") {
        build_main(matches)?;
        return Ok(true);
    } else if let Some(matches) = args.subcommand_matches("show") {
        show_main(matches)?;
        return Ok(true);
    }
    Ok(false)
}

pub fn entry_point(args: Vec<String>) -> AppResult<()> {
    let yaml = load_yaml!("cli.yml");
    let mut app = App::from_yaml(yaml).version(d4tools::VERSION);

    let matches = app.clone().get_matches_from(args);

    if !main_impl(matches)? {
        app.print_long_help()?;
    }
    Ok(())
}
