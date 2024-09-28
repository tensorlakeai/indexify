use std::error::Error;

use vergen::{BuildBuilder, Emitter, SysinfoBuilder};

fn main() -> Result<(), Box<dyn Error>> {
    let build = BuildBuilder::all_build()?;
    let si = SysinfoBuilder::all_sysinfo()?;

    Emitter::default()
        .add_instructions(&build)?
        .add_instructions(&si)?
        .emit()?;

    Ok(())
}
