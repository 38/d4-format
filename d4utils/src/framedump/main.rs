use d4_framefile::mode::ReadOnly;
use d4_framefile::*;
use std::fs::File;
use std::io::Write;

pub fn dump_dir(dir: &Directory<'static, ReadOnly, File>) {
    println!(
        "{:20}\t{:8}\t{}\t{}\n",
        "Name", "Type", "    Offset", "Primary-Size"
    );
    for entry in dir.entries() {
        println!(
            "{:20}\t{:8}\t{:10}\t{:10}",
            entry.name,
            match entry.kind {
                EntryKind::StreamCluster => "[SUBDIR]",
                EntryKind::FixedSized => "[BLOCK]",
                EntryKind::VariantLengthStream => "[STREAM]",
            },
            entry.primary_offset,
            entry.primary_size
        );
    }
}
pub fn entry_point(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::open(args.get(1).map_or("out.d4", |x| x.as_ref())).unwrap();
    let path: Vec<_> = args
        .get(2)
        .map_or("", |x| x.as_ref())
        .split(|x| x == '/')
        .collect();
    let mut dir = Directory::open_directory(file, 8).unwrap();
    let lc = path.len();
    if path == [""] {
        dump_dir(&dir);
    }
    for (idx, name) in path.iter().enumerate() {
        let last = lc - idx == 1;
        let kind = dir.entry_kind(name);
        match kind {
            Some(EntryKind::StreamCluster) if !last => {
                dir = dir.open_cluster_ro(name)?;
            }
            None | Some(_) if !last => {
                panic!("Path not found");
            }
            Some(EntryKind::VariantLengthStream) => {
                let mut stream = dir.open_stream_ro(name)?;
                let mut buf = vec![0; 4096];
                while let Ok(len) = stream.read(&mut buf) {
                    std::io::stdout().write_all(&buf[..len])?;
                    if len < 4096 {
                        break;
                    }
                }
            }
            Some(EntryKind::FixedSized) => {
                let chunk = dir.open_chunk_ro(name)?;
                std::io::stdout().write_all(chunk.mmap()?.as_ref())?;
            }
            Some(EntryKind::StreamCluster) => {
                let dir = dir.open_cluster_ro(name)?;
                dump_dir(&dir);
            }
            _ => {}
        }
    }
    Ok(())
}
