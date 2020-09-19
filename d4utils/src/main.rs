mod create;
mod framedump;
mod plot;
mod show;
mod stat;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "prof")]
    cpuprofiler::PROFILER
        .lock()
        .unwrap()
        .start("./d4utils.profile")?;
    let args: Vec<_> = std::env::args().skip(1).collect();
    let ret = match args.get(0).map(AsRef::as_ref) {
        Some("create") => create::entry_point(args),
        Some("framedump") => framedump::entry_point(args),
        Some("show") | Some("view") => show::entry_point(args),
        Some("stat") => stat::entry_point(args),
        Some("plot") => plot::entry_point(args),
        _ => {
            eprintln!("D4 Utilities Program");
            eprintln!("Usage: d4utils <subcommnd> <args>");
            eprintln!("Possible subcommands are:");
            eprintln!("\tcreate   \tCreate a new D4 depth profile");
            eprintln!("\tframedump\tDump The container data");
            eprintln!("\tview     \tPrint the underlying depth profile");
            eprintln!("\tstat     \tRun statistics on the given file");
            eprintln!("\tplot     \tPlot the specified region");
            Ok(())
        }
    };
    #[cfg(feature = "prof")]
    cpuprofiler::PROFILER.lock().unwrap().stop()?;
    ret
}
