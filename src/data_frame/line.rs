use crate::DataFrame;

use super::{indexing::DataFrameColumnIndex, Data, InnerData};

pub struct Line<'df> {
    df: &'df DataFrame,
    line: &'df Vec<InnerData>,
    index_map: &'df [usize],
}

impl<'df> IntoIterator for &Line<'df> {
    type Item = Data<'df>;

    type IntoIter = impl Iterator<Item = Data<'df>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'df> Line<'df> {
    pub(super) fn new(
        df: &'df DataFrame,
        line: &'df Vec<InnerData>,
        index_map: &'df [usize],
    ) -> Line<'df> {
        Line {
            df,
            line,
            index_map,
        }
    }

    pub(super) fn with_index_map(mut self, index_map: &'df [usize]) -> Line<'df> {
        self.index_map = index_map;
        self
    }

    pub fn iter(&self) -> impl Iterator<Item = Data<'df>> + 'df {
        self.index_map
            .iter()
            .map(|index| self.line[*index].as_data(self.df))
    }

    pub fn header(&self) -> impl Iterator<Item = &'df str> + '_ {
        self.index_map.iter().map(|index| {
            self.df
                .get_on_header(*index)
                .expect("index map out ouf bound")
        })
    }

    pub fn get<I>(&self, index: &I) -> Data<'df>
    where
        I: DataFrameColumnIndex + ?Sized,
    {
        let index = index.get_usize(self.header());
        self.line[index].as_data(self.df)
    }
}
