mod create;
mod framedump;
mod show;
mod stat;
mod plot;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "prof")]
    cpuprofiler::PROFILER
        .lock()
        .unwrap()
        .start("./d4utils.profile");
    let args: Vec<_> = std::env::args().skip(1).collect();
    let ret = match args.get(0).map(AsRef::as_ref) {
        Some("create") => create::entry_point(args),
        Some("framedump") => framedump::entry_point(args),
        Some("show") => show::entry_point(args),
        Some("stat") => stat::entry_point(args),
        Some("plot") => plot::entry_point(args),
        _ => panic!("Subcommand: create, framedump, show, stat, plot"),
    };
    #[cfg(feature = "prof")]
    cpuprofiler::PROFILER.lock().unwrap().stop();
    ret
}
