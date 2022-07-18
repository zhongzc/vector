use vector_core::event::LogEvent;

pub trait UpstreamEventParser {
    type UpstreamEvent;

    fn parse(&self, response: Self::UpstreamEvent, instance: String) -> Vec<LogEvent>;
}
