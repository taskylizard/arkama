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

pub fn run<T>(args: CompletionsArgs, binary_name: &str) -> Result<()>
where
    T: CommandFactory,
{
    let mut cmd = T::command();
    match args.shell {
        ShellChoice::Bash => generate_shell(Shell::Bash, &mut cmd, binary_name),
        ShellChoice::Zsh => generate_shell(Shell::Zsh, &mut cmd, binary_name),
        ShellChoice::Fish => generate_shell(Shell::Fish, &mut cmd, binary_name),
        ShellChoice::PowerShell => generate_shell(Shell::PowerShell, &mut cmd, binary_name),
        ShellChoice::Nushell => generate_nushell(&mut cmd),
    }
    Ok(())
}

fn generate_shell(shell: Shell, cmd: &mut clap::Command, binary_name: &str) {
    generate(shell, cmd, binary_name, &mut io::stdout());
}

fn generate_nushell(cmd: &mut clap::Command) {
    Nushell.generate(cmd, &mut io::stdout());
}
