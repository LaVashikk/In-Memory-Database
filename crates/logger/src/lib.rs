pub use log::{error, warn};

#[macro_export]
#[cfg(feature = "enable-info-logs")]
macro_rules! info {
    ($($arg:tt)+) => {
        $crate::log::info!($($arg)+)
    };
    () => {
        $crate::log::info!()
    };
}

#[macro_export]
#[cfg(not(feature = "enable-info-logs"))]
macro_rules! info {
    ($($arg:tt)+) => {};
    () => {};
}

#[macro_export]
#[cfg(feature = "enable-debug-logs")]
macro_rules! debug {
    ($($arg:tt)+) => {
        $crate::log::debug!($($arg)+)
    };
    () => {
        $crate::log::debug!()
    };
}

#[macro_export]
#[cfg(not(feature = "enable-debug-logs"))]
macro_rules! debug {
    ($($arg:tt)+) => {};
    () => {};
}

// Re-export the log crate to make it accessible inside the macros.
#[doc(hidden)]
pub use log;
