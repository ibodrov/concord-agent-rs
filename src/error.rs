#[derive(Debug)]
pub struct AppError {
    pub message: String,
}

impl AppError {
    pub fn new(message: &str) -> Self {
        Self {
            message: message.to_string(),
        }
    }
}

#[macro_export]
macro_rules! app_error {
    ($($arg:tt)*) => {
        $crate::error::AppError {
            message: format!($($arg)*),
        }
    };
}

#[macro_export]
macro_rules! app_err {
    ($($arg:tt)*) => {
        Err($crate::app_error!($($arg)*))
    };
}

impl std::error::Error for AppError {}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
