use clap::{load_yaml, App};
use serde_derive::Deserialize;
use warp::Filter;

use d4::{task::Task, D4TrackReader};

use std::io::Write;

#[derive(Deserialize)]
struct D4ServerQuery {
    #[serde(default)]
    class: String,
    #[serde(default)]
    chr: String,
    #[serde(default)]
    start: i64,
    #[serde(default)]
    end: i64,
}

async fn main(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml)
        .version(d4tools::VERSION)
        .get_matches_from(args);
    let path = matches.value_of("input-file").unwrap().to_string();
    let server_filter = warp::get()
        .and(warp::query::<D4ServerQuery>())
        .map(move |query| {
            let mut d4file: D4TrackReader = D4TrackReader::open(&path).unwrap();
            match query {
                D4ServerQuery { class, .. } if class == "header" => {
                    let header = d4file.header();
                    let chrom_list: Vec<_> = header
                        .chrom_list()
                        .iter()
                        .map(|chrom| &chrom.name)
                        .collect();
                    warp::http::Response::builder()
                        .header("content-type", "application/json")
                        .body(serde_json::to_vec(&chrom_list).unwrap())
                }
                D4ServerQuery {
                    class: _,
                    chr,
                    start,
                    end,
                } => {
                    let step = ((end - start + 999) / 1000).max(1);
                    let regions: Vec<_> = (0..)
                        .take_while(|x| step * x + start < end)
                        .map(|x| {
                            (
                                chr.as_str(),
                                (step * x + start) as u32,
                                (step * x + start + step).min(end) as u32,
                            )
                        })
                        .collect();
                    let task = d4::task::Mean::create_task(&mut d4file, &regions).unwrap();
                    let task_result = task.run();
                    let mut buffer = Vec::new();
                    buffer.write_all(&(start as u32).to_le_bytes()).unwrap();
                    buffer.write_all(&(step as u32).to_le_bytes()).unwrap();
                    buffer
                        .write_all(&(task_result.into_iter().len() as u32).to_le_bytes())
                        .unwrap();
                    task_result.into_iter().for_each(|out| {
                        buffer
                            .write_all(&(*out.output as f32).to_le_bytes())
                            .unwrap()
                    });
                    warp::http::Response::builder()
                        .header("content-type", "application/octect-stream")
                        .body(buffer)
                }
            }
        });
    warp::serve(server_filter).run(([0, 0, 0, 0], 60000)).await;
    Ok(())
}

pub fn entry_point(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async { main(args).await.expect("Server error") });
    Ok(())
}
