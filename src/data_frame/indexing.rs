pub trait DataFrameColumnIndex {
    #[doc(hidden)]
    fn get_usize(&self, header: &[String]) -> usize;
}

impl DataFrameColumnIndex for usize {
    fn get_usize(&self, _header: &[String]) -> usize {
        *self
    }
}

impl<'s> DataFrameColumnIndex for &'s str {
    fn get_usize(&self, header: &[String]) -> usize {
        if let Some((index, _)) = header
            .iter()
            .enumerate()
            .find(|(_i, string)| self == string)
        {
            index
        } else {
            panic!(
                "index out of Bound header is {:?} but index was '{}'",
                header, self
            )
        }
    }
}
