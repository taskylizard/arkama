use owo_colors::OwoColorize;

#[tokio::main]
async fn main() {
    if let Err(err) = arkama::run_gui_app().await {
        eprintln!("{} {err:#}", "error:".red().bold());
        std::process::exit(1);
    }
}
