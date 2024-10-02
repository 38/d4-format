mod create;
mod framedump;
mod index;
mod ls_track;
mod merge;
mod plot;
#[cfg(feature = "d4-server")]
mod server;
mod show;
mod stat;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "warn"),
    );
    let args: Vec<_> = std::env::args().skip(1).collect();
    let ret = match args.first().map(AsRef::as_ref) {
        Some("create") => create::entry_point(args),
        Some("framedump") => framedump::entry_point(args),
        Some("index") => index::entry_point(args),
        Some("ls-track") => ls_track::entry_point(args),
        Some("merge") => merge::entry_point(args),
        Some("plot") => plot::entry_point(args),
        #[cfg(feature = "d4-server")]
        Some("serve") => server::entry_point(args),
        Some("show") | Some("view") => show::entry_point(args),
        Some("stat") => stat::entry_point(args),
        _ => {
            eprintln!("D4 Utilities Program {}(D4 library version: {})", d4tools::VERSION, d4::VERSION);
            eprintln!("Usage: d4tools <subcommand> <args>");
            eprintln!("Possible subcommands are:");
            eprintln!("\tcreate   \tCreate a new D4 depth profile");
            eprintln!("\tframedump\tDump The container data");
            eprintln!("\tindex    \tIndex related operations");
            eprintln!("\tls-track \tList all available tracks in the D4 file");
            eprintln!("\tmerge    \tMerge existing D4 file as a multi-track D4 file");
            eprintln!("\tplot     \tPlot the specified region");
            #[cfg(feature = "d4-server")]
            eprintln!("\tserve    \tStart a D4 server");
            eprintln!("\tshow     \tPrint the underlying depth profile");
            eprintln!("\tstat     \tRun statistics on the given file");
            eprintln!("\tview     \tSame as show");
            eprintln!();
            eprintln!("Type 'd4tools <subcommand> --help' to learn more about each subcommands.");
            Ok(())
        }
    };

    if let Some(io_error) = ret
        .as_ref()
        .err()
        .and_then(|e| e.downcast_ref::<std::io::Error>())
    {
        if io_error.kind() == std::io::ErrorKind::BrokenPipe {
            return Ok(());
        }
    }
    ret
}
