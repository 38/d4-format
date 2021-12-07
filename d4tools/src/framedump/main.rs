use d4::ssio::http::HttpReader;
use d4_framefile::*;
use d4tools::AppResult;
use std::fs::File;
use std::io::{Read, Seek, Write};

#[allow(clippy::print_literal)]
pub fn dump_dir<R: Read + Seek>(dir: &Directory<R>) {
    println!(
        "{:20}\t{:8}\t{}\t{}\n",
        "Name", "Type", "    Offset", "Primary-Size"
    );
    for entry in dir.entries() {
        println!(
            "{:20}\t{:8}\t{:10}\t{:10}",
            entry.name,
            match entry.kind {
                EntryKind::SubDir => "[SUBDIR]",
                EntryKind::Blob => "[BLOB]",
                EntryKind::Stream => "[STREAM]",
            },
            entry.primary_offset,
            entry.primary_size
        );
    }
}
fn show_impl<R: Read + Seek>(stream: R, path: Vec<&str>) -> AppResult<()> {
    let mut dir = Directory::open_root(stream, 8).unwrap();
    let lc = path.len();
    if path == [""] {
        dump_dir(&dir);
    }
    for (idx, name) in path.iter().enumerate() {
        let last = lc - idx == 1;
        let kind = dir.entry_kind(name);
        match kind {
            Some(EntryKind::SubDir) if !last => {
                dir = dir.open_directory(name)?;
            }
            None | Some(_) if !last => {
                panic!("Path not found");
            }
            Some(EntryKind::Stream) => {
                let mut stream = dir.open_stream(name)?;
                let mut buf = vec![0; 4096];
                while let Ok(len) = stream.read(&mut buf) {
                    std::io::stdout().write_all(&buf[..len])?;
                    if len < 4096 {
                        break;
                    }
                }
            }
            Some(EntryKind::Blob) => {
                let mut blob = dir.open_blob(name)?;
                let mut offset = 0;
                let mut buffer = [0; 40960];
                let mut stdout = std::io::stdout();
                while offset < blob.size() {
                    let actual_read = blob.read_block(offset as u64, &mut buffer)?;
                    offset += actual_read;
                    stdout.write_all(&buffer[..actual_read])?;
                }
            }
            Some(EntryKind::SubDir) => {
                let dir = dir.open_directory(name)?;
                dump_dir(&dir);
            }
            _ => {}
        }
    }
    Ok(())
}
pub fn entry_point(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let frame_path: Vec<_> = args
        .get(2)
        .map_or("", |x| x.as_ref())
        .split(|x| x == '/')
        .collect();
    match args.get(1) {
        Some(url) if url.starts_with("http://") || url.starts_with("https://") => {
            let http_reader = HttpReader::new(url)?;
            show_impl(http_reader, frame_path)
        }
        Some(fs_path) => {
            let file_reader = File::open(fs_path)?;
            show_impl(file_reader, frame_path)
        }
        _ => panic!("Usage: d4tools framedump <path-or-url> <frame-path>"),
    }
}
