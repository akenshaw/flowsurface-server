use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
};

use anyhow::Error;

pub fn setup(is_debug: bool) -> Result<(), Error> {
    let level_filter = std::env::var("RUST_LOG")
        .ok()
        .as_deref()
        .map(str::parse::<log::Level>)
        .transpose()?
        .unwrap_or(log::Level::Debug)
        .to_level_filter();

    let mut io_sink = fern::Dispatch::new().format(|out, message, record| {
        out.finish(format_args!(
            "{}:{} -- {}",
            chrono::Local::now().format("%H:%M:%S%.3f"),
            record.level(),
            message
        ))
    });

    if is_debug {
        io_sink = io_sink.chain(io::stdout());
    } else {
        let log_file = PathBuf::from("logs/current.log");
        fs::create_dir_all(log_file.parent().unwrap())?;
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_file)?;
        io_sink = io_sink.chain(file);
    }

    fern::Dispatch::new()
        .level(log::LevelFilter::Off)
        .level_for("panic", log::LevelFilter::Error)
        .level_for("iced_wgpu", log::LevelFilter::Info)
        .level_for("exchanges", level_filter)
        .level_for("flowsurface", level_filter)
        .chain(io_sink)
        .apply()?;

    Ok(())
}
