use anyhow::Context;
pub use anyhow::Result;

pub trait ContextWithLocation<T> {
    fn context(self, msg: impl std::fmt::Display) -> Result<T>;
}

impl<T> ContextWithLocation<T> for Result<T> {
    #[track_caller]
    fn context(self, msg: impl std::fmt::Display) -> Result<T> {
        let loc = std::panic::Location::caller();
        self.with_context(|| format!("{} \n  at {}:{}", msg, loc.file(), loc.line()))
    }
}
