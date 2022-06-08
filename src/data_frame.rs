use std::iter::FusedIterator;

use std::ops::{Deref, Range};
use std::sync::Arc;
use std::{collections::HashMap, hash::Hash};

mod data;
use data::InnerData;
pub use data::{Data, SimpleDateTime};
mod line;
pub use line::Line;
mod group;
pub use group::Groups;

mod indexing;
use indexing::DataFrameColumnIndex;

mod display;
mod file_io;
mod frame_extension;

pub struct BaseDataFrame {
    string_storage: String,
    identity_index_map: Vec<usize>,
    header: Vec<Range<usize>>,
    data: Vec<Vec<InnerData>>,
}

pub struct DataFrame {
    inner: Arc<InnerDataFrame>,
}

impl Clone for DataFrame {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl From<InnerDataFrame> for DataFrame {
    fn from(inner: InnerDataFrame) -> Self {
        DataFrame {
            inner: Arc::new(inner),
        }
    }
}

impl From<Arc<InnerDataFrame>> for DataFrame {
    fn from(inner: Arc<InnerDataFrame>) -> Self {
        DataFrame { inner }
    }
}

enum InnerDataFrame {
    Base {
        df: BaseDataFrame,
    },
    ColumnReorder {
        df: DataFrame,
        index_map: Vec<usize>,
    },
    LineReorder {
        df: DataFrame,
        index_map: Vec<usize>,
    },
}

impl From<DataFrame> for BaseDataFrame {
    fn from(df: DataFrame) -> Self {
        let arc_df: DataFrame = match Arc::try_unwrap(df.inner) {
            Ok(InnerDataFrame::Base { df }) => {
                return df;
            }

            Err(df) => df.into(),

            Ok(df) => df.into(),
        };

        let mut header = Vec::new();
        let mut string_storage = String::new();
        for string in arc_df.header() {
            let start = string_storage.len();
            string_storage.push_str(string);
            let end = string_storage.len();
            header.push(start..end);
        }

        let data = arc_df
            .iter()
            .map(|line| {
                line.iter()
                    .map(|data| data.into_inner_data(&mut string_storage))
                    .collect::<Vec<InnerData>>()
            })
            .collect::<Vec<_>>();

        BaseDataFrame {
            string_storage,
            identity_index_map: (0..header.len()).collect(),
            header,
            data,
        }
    }
}

impl DataFrame {
    pub fn new(header: Vec<impl Into<String>>) -> DataFrame {
        let mut final_header = Vec::new();
        let mut string_storage = String::new();
        for string in header {
            let start = string_storage.len();
            string_storage.push_str(&string.into());
            let end = string_storage.len();
            final_header.push(start..end);
        }

        let df = BaseDataFrame {
            string_storage,
            identity_index_map: (0..final_header.len()).collect(),
            header: final_header,
            data: Vec::new(),
        };
        InnerDataFrame::Base { df }.into()
    }

    pub fn head(self, lines: usize) -> DataFrame {
        if lines < self.len() {
            let index_map = (0..lines).collect();
            InnerDataFrame::LineReorder {
                df: self,
                index_map,
            }
            .into()
        } else {
            self
        }
    }

    pub fn tail(self, lines: usize) -> DataFrame {
        if lines < self.len() {
            let index_map = (self.len() - lines..self.len()).collect();
            InnerDataFrame::LineReorder {
                df: self,
                index_map,
            }
            .into()
        } else {
            self
        }
    }

    pub fn range(self, start: usize, end: usize) -> DataFrame {
        assert!(start <= end);
        assert!(end <= self.len());

        let index_map = (start..end).collect();

        InnerDataFrame::LineReorder {
            df: self,
            index_map,
        }
        .into()
    }

    pub fn len(&self) -> usize {
        match self.inner.deref() {
            InnerDataFrame::Base { df } => df.data.len(),
            InnerDataFrame::LineReorder { index_map, .. } => index_map.len(),
            InnerDataFrame::ColumnReorder { df, .. } => df.len(),
        }
    }

    pub fn num_columns(&self) -> usize {
        match self.inner.deref() {
            InnerDataFrame::Base { df } => df.header.len(),
            InnerDataFrame::LineReorder { df, .. } => df.num_columns(),
            InnerDataFrame::ColumnReorder { index_map, .. } => index_map.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn sort<F, K>(self, mut key_gen: F) -> DataFrame
    where
        F: FnMut(Line) -> K,
        K: Ord,
    {
        let mut index_map = (0..self.len()).collect::<Vec<_>>();
        index_map.sort_by_key(|index| {
            let line = self
                .get(*index)
                .expect("unreachable since index_map is 0 to len");
            key_gen(line)
        });

        InnerDataFrame::LineReorder {
            df: self,
            index_map,
        }
        .into()
    }

    pub fn drop_column<I>(self, index: I) -> DataFrame
    where
        I: DataFrameColumnIndex,
    {
        let index_to_remove = index.get_usize(self.header());
        InnerDataFrame::ColumnReorder {
            index_map: (0..self.num_columns())
                .filter(|index| *index != index_to_remove)
                .collect(),
            df: self,
        }
        .into()
    }

    pub fn drop_all_column_except<I>(self, indizes: &[I]) -> DataFrame
    where
        I: DataFrameColumnIndex,
    {
        let to_keep = indizes.iter().map(|i| i.get_usize(self.header())).collect();
        InnerDataFrame::ColumnReorder {
            df: self,
            index_map: to_keep,
        }
        .into()
    }

    pub fn fold_column<I, T, F>(&self, index: &I, init: T, f: F) -> T
    where
        I: DataFrameColumnIndex,
        F: FnMut(T, Data) -> T,
    {
        self.iter().map(|line| line.get(index)).fold(init, f)
    }

    pub fn filter<F>(self, mut filter: F) -> DataFrame
    where
        F: FnMut(Line) -> bool,
    {
        let index_map = self
            .iter()
            .enumerate()
            .filter_map(|(i, line)| if filter(line) { Some(i) } else { None })
            .collect();

        InnerDataFrame::LineReorder {
            df: self,
            index_map,
        }
        .into()
    }

    pub fn group_by<F, G>(self, mut grouper: F) -> Groups<G>
    where
        F: FnMut(Line) -> G,
        G: Hash + Eq,
    {
        let mut map = HashMap::new();
        for (i, line) in self.iter().enumerate() {
            let key = grouper(line);
            let vec: &mut Vec<_> = map.entry(key).or_default();
            vec.push(i);
        }
        let mut map = map.drain().collect::<Vec<_>>();
        //sort so we do not get non determinismus by using a Hashmap
        map.sort_by_key(|(_g, vec)| vec[0]);
        let mut groups = Vec::new();
        for (key, index_map) in map {
            groups.push((
                key,
                InnerDataFrame::LineReorder {
                    df: self.clone(),
                    index_map,
                }
                .into(),
            ));
        }
        Groups::new(groups)
    }

    pub fn header(&self) -> HeaderIter {
        HeaderIter::new(self)
    }

    pub fn iter(&self) -> LineIter {
        LineIter::new(self)
    }

    pub fn get(&self, index: usize) -> Option<Line> {
        match self.inner.deref() {
            InnerDataFrame::Base { df } => df
                .data
                .get(index)
                .map(|line| Line::new(self, line, &df.identity_index_map)),
            InnerDataFrame::LineReorder { df, index_map } => {
                index_map.get(index).and_then(|index| df.get(*index))
            }

            InnerDataFrame::ColumnReorder { df, index_map } => {
                let line = df.get(index);
                line.map(|line| line.with_index_map(index_map))
            }
        }
    }

    fn get_on_header(&self, index: usize) -> Option<&str> {
        match self.inner.deref() {
            InnerDataFrame::Base { df, .. } => df.header.get(index).map(|range| {
                df.string_storage
                    .get(range.clone())
                    .expect("Header index inconsitant with string_storage UTF8 boundary")
            }),
            InnerDataFrame::LineReorder { df, .. } => df.get_on_header(index),
            InnerDataFrame::ColumnReorder { df, index_map } => index_map
                .get(index)
                .and_then(|index| df.get_on_header(*index)),
        }
    }

    fn get_from_string_storage(&self, range: Range<usize>) -> &str {
        match self.inner.deref() {
            InnerDataFrame::Base { df, .. } => &df.string_storage[range],
            InnerDataFrame::LineReorder { df, .. } | InnerDataFrame::ColumnReorder { df, .. } => {
                df.get_from_string_storage(range)
            }
        }
    }
}

pub struct LineIter<'df> {
    df: &'df DataFrame,
    index: usize,
    end: usize,
}

impl<'df> LineIter<'df> {
    fn new(df: &'df DataFrame) -> LineIter<'df> {
        LineIter {
            end: df.len(),
            df,
            index: 0,
        }
    }
}

impl<'df> FusedIterator for LineIter<'df> {}
impl<'df> ExactSizeIterator for LineIter<'df> {}
impl<'df> DoubleEndedIterator for LineIter<'df> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index < self.end {
            let item = self.df.get(self.end - 1);
            assert!(item.is_some());
            self.end -= 1;
            item
        } else {
            None
        }
    }
}

impl<'df> Iterator for LineIter<'df> {
    type Item = Line<'df>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.end {
            let item = self.df.get(self.index);
            assert!(item.is_some());
            self.index += 1;
            item
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.end - self.index;
        (size, Some(size))
    }
}

impl<'df> IntoIterator for &'df DataFrame {
    type Item = Line<'df>;

    type IntoIter = LineIter<'df>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct HeaderIter<'df> {
    df: &'df DataFrame,
    index: usize,
    end: usize,
}

impl<'df> HeaderIter<'df> {
    fn new(df: &'df DataFrame) -> HeaderIter<'df> {
        HeaderIter {
            index: 0,
            end: df.num_columns(),
            df,
        }
    }
}

impl<'df> FusedIterator for HeaderIter<'df> {}
impl<'df> ExactSizeIterator for HeaderIter<'df> {}
impl<'df> DoubleEndedIterator for HeaderIter<'df> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index < self.end {
            let item = self.df.get_on_header(self.end - 1);
            assert!(item.is_some());
            self.end -= 1;
            item
        } else {
            None
        }
    }
}

impl<'df> Iterator for HeaderIter<'df> {
    type Item = &'df str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.end {
            let item = self.df.get_on_header(self.index);
            assert!(item.is_some());
            self.index += 1;
            item
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.end - self.index;
        (size, Some(size))
    }
}
