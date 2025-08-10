use anyhow::Result;

pub trait ContainerDriver {
    fn new(container_id: String) -> Result<()>;

    fn stop(&self) -> Result<()>;
}