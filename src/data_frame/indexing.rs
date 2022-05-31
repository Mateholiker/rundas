pub trait DataFrameColumnIndex {
    #[doc(hidden)]
    fn get_usize<'a>(&self, header: impl Iterator<Item = &'a str>) -> usize;
}

impl DataFrameColumnIndex for usize {
    fn get_usize<'a>(&self, _header: impl Iterator<Item = &'a str>) -> usize {
        *self
    }
}

impl<'s> DataFrameColumnIndex for &'s str {
    fn get_usize<'a>(&self, header: impl Iterator<Item = &'a str>) -> usize {
        if let Some((index, _)) = header.enumerate().find(|(_i, string)| self == string) {
            index
        } else {
            panic!("index out of Bound: Header does not contain '{self}'")
        }
    }
}
