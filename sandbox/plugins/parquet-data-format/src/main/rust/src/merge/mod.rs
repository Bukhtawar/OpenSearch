mod context;
mod cursor;
pub mod error;
pub mod heap;
pub mod io_task;
pub mod schema;
mod sorted;
mod unsorted;
pub mod schema_optimized;
pub mod row_compare;
pub mod cursor_optimized;
pub mod sorted_optimized;

pub use error::{MergeError, MergeResult};
pub use sorted::merge_sorted;
pub use unsorted::merge_unsorted;
pub use sorted_optimized::merge_sorted_optimized;
