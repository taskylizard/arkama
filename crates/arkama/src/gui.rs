use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Instant;

use arkama_core::{
    DownloadControl, DownloadEvent, DownloadRequest, DownloadSummary, start_download_with_handle,
};
use arkama_data::{Db, DownloadRecord, default_download_dir};
use eframe::egui;
use eyre::{Context, Result};
use tokio::runtime::Handle;

use crate::util::{
    active_label, history_progress_text, progress_fraction, progress_text, should_persist_progress,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum StopReason {
    Pause,
    Cancel,
}

struct ActiveDownload {
    id: i64,
    url: String,
    receiver: tokio::sync::mpsc::UnboundedReceiver<DownloadEvent>,
    done_rx: mpsc::Receiver<eyre::Result<DownloadSummary>>,
    control: DownloadControl,
    output: PathBuf,
    total_bytes: Option<u64>,
    downloaded_bytes: u64,
    last_db_update: Instant,
    last_db_bytes: u64,
    stop_reason: Option<StopReason>,
}

struct PendingDownload {
    url: String,
    output: Option<PathBuf>,
}

pub fn run() -> Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    let options = eframe::NativeOptions::default();
    let app = ArkamaApp::new()?;
    eframe::run_native("Arkama", options, Box::new(|_cc| Box::new(app)))
        .map_err(|err| eyre::eyre!(err.to_string()))?;
    Ok(())
}

struct ArkamaApp {
    runtime: Handle,
    db: Db,
    url_input: String,
    download_dir: PathBuf,
    download_dir_input: String,
    search_query: String,
    downloads: Vec<DownloadRecord>,
    active: Vec<ActiveDownload>,
    pending: VecDeque<PendingDownload>,
    max_concurrent: usize,
    connections: usize,
    speed_limit_input: String,
    user_agent_input: String,
    show_settings: bool,
    show_onboarding: bool,
    dark_mode: bool,
    status_message: Option<String>,
    refresh_needed: bool,
}

impl ArkamaApp {
    fn new() -> Result<Self> {
        let runtime = Handle::current();
        let db = Db::open()?;
        db.normalize_running_to_paused()?;
        let download_dir = db
            .get_setting("download_dir")?
            .map(PathBuf::from)
            .unwrap_or_else(default_download_dir);
        let max_concurrent = db
            .get_setting("max_concurrent")?
            .and_then(|v| v.parse().ok())
            .unwrap_or(3usize);
        let connections = db
            .get_setting("connections")?
            .and_then(|v| v.parse().ok())
            .unwrap_or(4usize);
        let speed_limit = db.get_setting("speed_limit")?.unwrap_or_default();
        let user_agent = db.get_setting("user_agent")?.unwrap_or_default();
        let dark_mode = db
            .get_setting("dark_mode")?
            .map(|v| v == "true")
            .unwrap_or(true);
        let onboarding_complete = db
            .get_setting("onboarding_complete")?
            .map(|v| v == "true")
            .unwrap_or(false);
        let downloads = db.load_downloads("")?;

        Ok(Self {
            runtime,
            db,
            url_input: String::new(),
            download_dir_input: download_dir.display().to_string(),
            download_dir,
            search_query: String::new(),
            downloads,
            active: Vec::new(),
            pending: VecDeque::new(),
            max_concurrent,
            connections,
            speed_limit_input: speed_limit,
            user_agent_input: user_agent,
            show_settings: false,
            show_onboarding: !onboarding_complete,
            dark_mode,
            status_message: None,
            refresh_needed: false,
        })
    }

    fn enqueue_download(&mut self, url: String, output: Option<PathBuf>) {
        self.pending.push_back(PendingDownload { url, output });
        if let Err(err) = self.schedule() {
            self.status_message = Some(format!("error: {err}"));
        }
    }

    fn schedule(&mut self) -> Result<()> {
        while self
            .active
            .iter()
            .filter(|active| active.stop_reason.is_none())
            .count()
            < self.max_concurrent
        {
            let Some(pending) = self.pending.pop_front() else {
                break;
            };
            self.start_one(pending)?;
        }
        Ok(())
    }

    fn start_one(&mut self, pending: PendingDownload) -> Result<()> {
        let output_dir = self.download_dir.clone();
        std::fs::create_dir_all(&output_dir)
            .with_context(|| format!("failed to create download dir {output_dir:?}"))?;

        let limit = if self.speed_limit_input.is_empty() {
            None
        } else {
            bytefmt::parse(&self.speed_limit_input).ok()
        };
        let user_agent = if self.user_agent_input.is_empty() {
            None
        } else {
            Some(self.user_agent_input.clone())
        };

        let request = DownloadRequest {
            url: pending.url.clone(),
            output: pending.output,
            output_dir: Some(output_dir),
            connections: self.connections,
            user_agent,
            limit,
            experimental_entropy: false,
        };
        let handle = start_download_with_handle(self.runtime.clone(), request)?;

        let id = self.db.insert_download(&pending.url, &PathBuf::new())?;
        let (done_tx, done_rx) = mpsc::channel();
        let join = handle.join;
        self.runtime.spawn(async move {
            let result: eyre::Result<DownloadSummary> = match join.await {
                Ok(inner) => inner,
                Err(err) => Err(eyre::eyre!(err)),
            };
            let _ = done_tx.send(result);
        });

        self.active.push(ActiveDownload {
            id,
            url: pending.url,
            receiver: handle.events,
            done_rx,
            control: handle.control.clone(),
            output: PathBuf::new(),
            total_bytes: None,
            downloaded_bytes: 0,
            last_db_update: Instant::now(),
            last_db_bytes: 0,
            stop_reason: None,
        });

        self.status_message = Some("download started".to_string());
        self.refresh_needed = true;
        Ok(())
    }

    fn start_download(&mut self) -> Result<()> {
        let url = self.url_input.trim().to_string();
        if url.is_empty() {
            self.status_message = Some("url required".to_string());
            return Ok(());
        }
        self.url_input.clear();
        self.enqueue_download(url, None);
        Ok(())
    }

    fn resume_download(&mut self, record: &DownloadRecord) {
        let output = if record.output_path.is_empty() {
            None
        } else {
            Some(PathBuf::from(&record.output_path))
        };
        self.enqueue_download(record.url.clone(), output);
    }

    fn save_download_dir(&mut self) -> Result<()> {
        let path = PathBuf::from(self.download_dir_input.trim());
        if path.as_os_str().is_empty() {
            self.status_message = Some("download folder required".to_string());
            return Ok(());
        }
        self.db
            .set_setting("download_dir", &path.display().to_string())?;
        self.download_dir = path;
        self.status_message = Some("download folder saved".to_string());
        Ok(())
    }

    fn refresh_downloads(&mut self) -> Result<()> {
        let query = self.search_query.trim();
        self.downloads = self.db.load_downloads(query)?;
        self.refresh_needed = false;
        Ok(())
    }

    fn poll_active(&mut self) -> Result<()> {
        let mut finished: Vec<(i64, DownloadSummary)> = Vec::new();
        let mut failed: Vec<(i64, String, Option<StopReason>)> = Vec::new();

        for active in &mut self.active {
            let mut processed = 0usize;
            while processed < 200 {
                match active.receiver.try_recv() {
                    Ok(event) => match event {
                        DownloadEvent::Started {
                            output,
                            total_bytes,
                            resumed_bytes,
                        } => {
                            active.output = output.clone();
                            active.total_bytes = total_bytes;
                            active.downloaded_bytes = resumed_bytes;
                            self.db.update_download_started(
                                active.id,
                                &output,
                                total_bytes,
                                resumed_bytes,
                            )?;
                            self.refresh_needed = true;
                        }
                        DownloadEvent::Progress {
                            downloaded_bytes,
                            total_bytes,
                        } => {
                            active.downloaded_bytes = downloaded_bytes;
                            if total_bytes.is_some() {
                                active.total_bytes = total_bytes;
                            }
                            if should_persist_progress(
                                &active.last_db_update,
                                active.last_db_bytes,
                                downloaded_bytes,
                            ) {
                                active.last_db_update = Instant::now();
                                active.last_db_bytes = downloaded_bytes;
                                self.db.update_download_progress(
                                    active.id,
                                    total_bytes,
                                    downloaded_bytes,
                                )?;
                                self.refresh_needed = true;
                            }
                        }
                        DownloadEvent::Finished { summary } => {
                            finished.push((active.id, summary));
                            self.refresh_needed = true;
                            break;
                        }
                        DownloadEvent::Failed { message } => {
                            failed.push((active.id, message, active.stop_reason));
                            self.refresh_needed = true;
                            break;
                        }
                    },
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                }
                processed += 1;
            }

            if active.stop_reason.is_none() {
                match active.done_rx.try_recv() {
                    Ok(Ok(summary)) => {
                        finished.push((active.id, summary));
                        self.refresh_needed = true;
                    }
                    Ok(Err(err)) => {
                        failed.push((active.id, err.to_string(), active.stop_reason));
                        self.refresh_needed = true;
                    }
                    Err(mpsc::TryRecvError::Empty) => {}
                    Err(mpsc::TryRecvError::Disconnected) => {}
                }
            }
        }

        for (id, summary) in finished {
            self.db.update_download_finished(id, &summary)?;
            self.active.retain(|a| a.id != id);
            self.send_notification("Download complete", &summary.output.display().to_string());
            self.status_message = Some("download finished".to_string());
        }

        for (id, message, stop_reason) in failed {
            match stop_reason {
                Some(StopReason::Pause) => {}
                Some(StopReason::Cancel) => {
                    if let Some(active) = self.active.iter().find(|active| active.id == id)
                        && active.output.exists()
                    {
                        let _ = std::fs::remove_file(&active.output);
                        let state_path = active.output.with_extension("arkama.state");
                        if state_path.exists() {
                            let _ = std::fs::remove_file(state_path);
                        }
                    }
                }
                None => {
                    if message != "interrupted" {
                        self.db.update_download_failed(id, &message)?;
                        self.status_message = Some(format!("download failed: {message}"));
                    }
                }
            }
            self.active.retain(|a| a.id != id);
        }

        self.schedule()?;
        Ok(())
    }

    fn send_notification(&self, title: &str, body: &str) {
        let _ = notify_rust::Notification::new()
            .summary(title)
            .body(body)
            .appname("Arkama")
            .show();
    }

    fn pause_download(&mut self, id: i64) {
        for active in &mut self.active {
            if active.id == id {
                active.stop_reason = Some(StopReason::Pause);
                active.control.pause();
                let _ = self.db.update_download_paused(id, active.downloaded_bytes);
                self.refresh_needed = true;
                self.status_message = Some("download paused".to_string());
                break;
            }
        }
    }

    fn cancel_download(&mut self, id: i64) {
        for active in &mut self.active {
            if active.id == id {
                active.stop_reason = Some(StopReason::Cancel);
                active.control.cancel();
                let _ = self.db.update_download_cancelled(id);
                self.refresh_needed = true;
                self.status_message = Some("download cancelled".to_string());
                break;
            }
        }
    }
}

impl eframe::App for ArkamaApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if self.dark_mode {
            ctx.set_visuals(egui::Visuals::dark());
        } else {
            ctx.set_visuals(egui::Visuals::light());
        }

        ctx.input(|i| {
            if i.modifiers.command
                && i.key_pressed(egui::Key::V)
                && let Some(text) = i.events.iter().find_map(|e| {
                    if let egui::Event::Paste(t) = e {
                        Some(t.clone())
                    } else {
                        None
                    }
                })
                && (text.starts_with("http://") || text.starts_with("https://"))
            {
                self.url_input = text;
            }
            if i.modifiers.command && i.key_pressed(egui::Key::Comma) {
                self.show_settings = !self.show_settings;
            }
            if i.key_pressed(egui::Key::Escape) {
                self.show_settings = false;
            }
        });

        if let Err(err) = self.poll_active() {
            self.status_message = Some(format!("error: {err}"));
        }
        if self.refresh_needed
            && let Err(err) = self.refresh_downloads()
        {
            self.status_message = Some(format!("error: {err}"));
        }

        if self.show_onboarding {
            egui::Window::new("Welcome to Arkama!")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.label("A fast, modern download manager.");
                    ui.add_space(10.0);

                    ui.label("Choose your download folder:");
                    ui.horizontal(|ui| {
                        ui.text_edit_singleline(&mut self.download_dir_input);
                        if ui.button("📁").clicked()
                            && let Some(path) = rfd::FileDialog::new()
                                .set_directory(&self.download_dir)
                                .pick_folder()
                        {
                            self.download_dir_input = path.display().to_string();
                        }
                    });

                    ui.add_space(10.0);
                    ui.label("Features:");
                    ui.label("• Pause, resume, and cancel downloads");
                    ui.label("• Multiple concurrent downloads");
                    ui.label("• Resume interrupted downloads");
                    ui.label("• Right-click for context menu");

                    ui.add_space(15.0);
                    ui.horizontal(|ui| {
                        if ui.button("Get Started").clicked() {
                            let path = PathBuf::from(self.download_dir_input.trim());
                            if !path.as_os_str().is_empty() {
                                let _ = self
                                    .db
                                    .set_setting("download_dir", &path.display().to_string());
                                self.download_dir = path;
                            }
                            let _ = self.db.set_setting("onboarding_complete", "true");
                            self.show_onboarding = false;
                        }
                        if ui.button("Skip").clicked() {
                            let _ = self.db.set_setting("onboarding_complete", "true");
                            self.show_onboarding = false;
                        }
                    });
                });
        }

        egui::Window::new("Settings")
            .open(&mut self.show_settings)
            .resizable(false)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.label("Max concurrent downloads:");
                    let mut max_str = self.max_concurrent.to_string();
                    if ui.text_edit_singleline(&mut max_str).changed()
                        && let Ok(val) = max_str.parse::<usize>()
                    {
                        let val = val.max(1);
                        self.max_concurrent = val;
                        let _ = self.db.set_setting("max_concurrent", &val.to_string());
                    }
                });
                ui.horizontal(|ui| {
                    ui.label("Connections per download:");
                    let mut conn_str = self.connections.to_string();
                    if ui.text_edit_singleline(&mut conn_str).changed()
                        && let Ok(val) = conn_str.parse::<usize>()
                    {
                        let val = val.max(1);
                        self.connections = val;
                        let _ = self.db.set_setting("connections", &val.to_string());
                    }
                });
                ui.horizontal(|ui| {
                    ui.label("Speed limit (e.g., 2MB, 500KB):");
                    if ui
                        .text_edit_singleline(&mut self.speed_limit_input)
                        .changed()
                    {
                        let _ = self.db.set_setting("speed_limit", &self.speed_limit_input);
                    }
                });
                ui.horizontal(|ui| {
                    ui.label("User-Agent:");
                    if ui
                        .text_edit_singleline(&mut self.user_agent_input)
                        .changed()
                    {
                        let _ = self.db.set_setting("user_agent", &self.user_agent_input);
                    }
                });
                ui.separator();
                if ui.checkbox(&mut self.dark_mode, "Dark mode").changed() {
                    let _ = self
                        .db
                        .set_setting("dark_mode", if self.dark_mode { "true" } else { "false" });
                }
            });

        egui::TopBottomPanel::top("controls").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.label("URL");
                ui.text_edit_singleline(&mut self.url_input);
                if ui.button("Download").clicked()
                    && let Err(err) = self.start_download()
                {
                    self.status_message = Some(format!("error: {err}"));
                }
            });
            ui.horizontal(|ui| {
                ui.label("Downloads folder");
                ui.text_edit_singleline(&mut self.download_dir_input);
                if ui.button("📁").clicked()
                    && let Some(path) = rfd::FileDialog::new()
                        .set_directory(&self.download_dir)
                        .pick_folder()
                {
                    self.download_dir_input = path.display().to_string();
                    if let Err(err) = self.save_download_dir() {
                        self.status_message = Some(format!("error: {err}"));
                    }
                }
                if ui.button("Save").clicked()
                    && let Err(err) = self.save_download_dir()
                {
                    self.status_message = Some(format!("error: {err}"));
                }
            });
            ui.horizontal(|ui| {
                ui.label("Search");
                let before = self.search_query.clone();
                ui.text_edit_singleline(&mut self.search_query);
                if before != self.search_query {
                    self.refresh_needed = true;
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("⚙").clicked() {
                        self.show_settings = !self.show_settings;
                    }
                });
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            let mut pause_ids = Vec::new();
            let mut cancel_ids = Vec::new();

            let mut copy_active_url: Option<String> = None;

            for active in &self.active {
                ui.group(|ui| {
                    ui.horizontal(|ui| {
                        let label = ui.label(active_label(&active.output));
                        label.context_menu(|ui| {
                            if ui.button("Copy URL").clicked() {
                                copy_active_url = Some(active.url.clone());
                                ui.close_menu();
                            }
                        });
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui.button("✕").clicked() {
                                cancel_ids.push(active.id);
                            }
                            if ui.button("⏸").clicked() {
                                pause_ids.push(active.id);
                            }
                        });
                    });
                    let total_bytes = active.total_bytes;
                    let fraction = progress_fraction(active.downloaded_bytes, total_bytes);
                    let text = progress_text(active.downloaded_bytes, total_bytes);
                    let mut bar = egui::ProgressBar::new(fraction).text(text);
                    if total_bytes.is_none() {
                        bar = bar.animate(true);
                    }
                    ui.add(bar);
                });
            }

            for id in pause_ids {
                self.pause_download(id);
            }
            for id in cancel_ids {
                self.cancel_download(id);
            }
            if let Some(url) = copy_active_url {
                ctx.copy_text(url);
            }

            if !self.active.is_empty() {
                ui.separator();
            }

            let mut resume_record: Option<DownloadRecord> = None;
            let mut delete_id: Option<i64> = None;
            let mut open_file: Option<PathBuf> = None;
            let mut reveal_file: Option<PathBuf> = None;
            let mut copy_url: Option<String> = None;

            egui::ScrollArea::vertical().show(ui, |ui| {
                egui::Grid::new("download_history")
                    .striped(true)
                    .spacing([12.0, 6.0])
                    .show(ui, |ui| {
                        ui.strong("URL");
                        ui.strong("Output");
                        ui.strong("Status");
                        ui.strong("Downloaded");
                        ui.strong("Actions");
                        ui.end_row();

                        for record in &self.downloads {
                            let row_response = ui.label(truncate_url(&record.url, 40));
                            row_response.context_menu(|ui| {
                                if ui.button("Copy URL").clicked() {
                                    copy_url = Some(record.url.clone());
                                    ui.close_menu();
                                }
                                if !record.output_path.is_empty() {
                                    if ui.button("Open file").clicked() {
                                        open_file = Some(PathBuf::from(&record.output_path));
                                        ui.close_menu();
                                    }
                                    if ui.button("Show in folder").clicked() {
                                        reveal_file = Some(PathBuf::from(&record.output_path));
                                        ui.close_menu();
                                    }
                                }
                                if record.status == "paused" && ui.button("Resume").clicked() {
                                    resume_record = Some(record.clone());
                                    ui.close_menu();
                                }
                                ui.separator();
                                if ui.button("Delete from history").clicked() {
                                    delete_id = Some(record.id);
                                    ui.close_menu();
                                }
                            });
                            ui.label(truncate_path(&record.output_path, 30));
                            ui.label(&record.status);
                            ui.label(history_progress_text(
                                record.downloaded_bytes,
                                record.total_bytes,
                            ));
                            ui.horizontal(|ui| {
                                if record.status == "paused" && ui.button("▶").clicked() {
                                    resume_record = Some(record.clone());
                                }
                                if ui.button("🗑").clicked() {
                                    delete_id = Some(record.id);
                                }
                            });
                            ui.end_row();
                        }
                    });
            });

            if let Some(record) = resume_record {
                self.resume_download(&record);
            }
            if let Some(id) = delete_id {
                let _ = self.db.delete_download(id);
                self.refresh_needed = true;
            }
            if let Some(url) = copy_url {
                ctx.copy_text(url);
            }
            if let Some(path) = open_file {
                let _ = open::that(&path);
            }
            if let Some(path) = reveal_file {
                let _ = open::that_detached(path.parent().unwrap_or(&path));
            }
        });

        egui::TopBottomPanel::bottom("status").show(ctx, |ui| {
            ui.horizontal(|ui| {
                if let Some(message) = self.status_message.as_ref() {
                    ui.label(message);
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(format!(
                        "Active: {} | Queued: {}",
                        self.active.len(),
                        self.pending.len()
                    ));
                });
            });
        });

        ctx.request_repaint_after(std::time::Duration::from_millis(100));
    }
}

fn truncate_url(url: &str, max_len: usize) -> String {
    if url.len() <= max_len {
        url.to_string()
    } else {
        format!("{}…", &url[..max_len - 1])
    }
}

fn truncate_path(path: &str, max_len: usize) -> String {
    if path.len() <= max_len {
        path.to_string()
    } else {
        let start = path.len().saturating_sub(max_len - 1);
        format!("…{}", &path[start..])
    }
}
