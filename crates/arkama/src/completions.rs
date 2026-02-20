use std::io;

use clap::{Args, CommandFactory, ValueEnum};
use clap_complete::{Generator, Shell, generate};
use clap_complete_nushell::Nushell;
use eyre::Result;

#[derive(Args, Debug)]
pub struct CompletionsArgs {
    #[arg(value_enum, help = "Shell to generate completions for.")]
    pub shell: ShellChoice,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ShellChoice {
    Bash,
    Zsh,
    Fish,
    #[value(name = "powershell")]
    PowerShell,
    Nushell,
}

pub fn run(args: CompletionsArgs) -> Result<()> {
    let mut cmd = crate::Args::command();
    match args.shell {
        ShellChoice::Bash => generate_shell(Shell::Bash, &mut cmd),
        ShellChoice::Zsh => generate_shell(Shell::Zsh, &mut cmd),
        ShellChoice::Fish => generate_shell(Shell::Fish, &mut cmd),
        ShellChoice::PowerShell => generate_shell(Shell::PowerShell, &mut cmd),
        ShellChoice::Nushell => generate_nushell(&mut cmd),
    }
    Ok(())
}

fn generate_shell(shell: Shell, cmd: &mut clap::Command) {
    generate(shell, cmd, "arkama", &mut io::stdout());
}

fn generate_nushell(cmd: &mut clap::Command) {
    Nushell.generate(cmd, &mut io::stdout());
}
