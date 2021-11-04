mod create;
mod framedump;
mod ls_track;
mod merge;
mod plot;
mod server;
mod show;
mod stat;
mod utils;
mod index;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let ret = match args.get(0).map(AsRef::as_ref) {
        Some("create") => create::entry_point(args),
        Some("framedump") => framedump::entry_point(args),
        Some("index") => index::entry_point(args),
        Some("ls-track") => ls_track::entry_point(args),
        Some("merge") => merge::entry_point(args),
        Some("plot") => plot::entry_point(args),
        Some("serve") => server::entry_point(args),
        Some("show") | Some("view") => show::entry_point(args),
        Some("stat") => stat::entry_point(args),
        _ => {
            eprintln!("D4 Utilities Program {}", d4::VERSION);
            eprintln!("Usage: d4tools <subcommand> <args>");
            eprintln!("Possible subcommands are:");
            eprintln!("\tcreate   \tCreate a new D4 depth profile");
            eprintln!("\tframedump\tDump The container data");
            eprintln!("\tindex    \tIndex related operations");
            eprintln!("\tls-track \tList all available tracks in the D4 file");
            eprintln!("\tmerge    \tMerge existing D4 file as a multi-track D4 file");
            eprintln!("\tplot     \tPlot the specified region");
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
