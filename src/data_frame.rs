use std::collections::BTreeSet;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::{collections::HashMap, hash::Hash, ops::Not};

use serde::{Deserialize, Serialize};

mod view;
pub use view::View;
use view::ViewType;
mod data;
pub use data::{Data, SimpleDateTime};
mod line;
pub use line::Line;
mod indexing;
use indexing::DataFrameColumnIndex;
mod frame_extension;
mod group;
pub use group::Groups;
mod file_io;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFrame {
    header: Vec<String>,
    data: Vec<Vec<Data>>,
}

impl DataFrame {
    pub fn new(mut header: Vec<impl Into<String>>) -> DataFrame {
        DataFrame {
            header: header.drain(..).map(|s| s.into()).collect(),
            data: Vec::new(),
        }
    }

    pub fn head(&self, lines: usize) -> View {
        View::new(self, ViewType::Head(lines))
    }

    pub fn tail(&self, lines: usize) -> View {
        View::new(self, ViewType::Tail(lines))
    }

    pub fn range(&self, start: usize, end: usize) -> View {
        View::new(self, ViewType::Range(start, end))
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn sort<F, K>(&mut self, mut key_gen: F)
    where
        F: FnMut(Line) -> K,
        K: Ord,
    {
        self.data.sort_by_key(|elem| {
            let line = Line::new(&self.header, elem);
            key_gen(line)
        });
    }

    pub fn drop_column<I>(&mut self, index: I)
    where
        I: DataFrameColumnIndex,
    {
        let index = index.get_usize(&self.header);
        self.header.remove(index);
        for line in self.data.iter_mut() {
            line.remove(index);
        }
    }

    pub fn drop_all_column_except<I>(&mut self, indizes: &[I])
    where
        I: DataFrameColumnIndex,
    {
        let to_keep: BTreeSet<usize> = indizes.iter().map(|i| i.get_usize(&self.header)).collect();
        let keep_mask = (0..self.header.len())
            .map(|i| to_keep.contains(&i))
            .collect::<Vec<bool>>();
        let mut keep_mask_iter = keep_mask.iter();
        self.header.retain(|_| *keep_mask_iter.next().unwrap());
        for line in self.data.iter_mut() {
            let mut keep_mask_iter = keep_mask.iter();
            line.retain(|_| *keep_mask_iter.next().unwrap());
        }
    }

    pub fn fold_column<I, T, F>(&self, index: I, init: T, f: F) -> T
    where
        I: DataFrameColumnIndex,
        F: FnMut(T, Data) -> T,
    {
        let index = index.get_usize(&self.header);
        self.data
            .iter()
            .map(|line| &line[index])
            .cloned()
            .fold(init, f)
    }

    pub fn filter<F>(&mut self, mut filter: F)
    where
        F: FnMut(Line) -> bool,
    {
        self.data.drain_filter(|line| {
            let line = Line::new(&self.header, line);
            !filter(line)
        });
    }

    pub fn filter_unstable<F>(&mut self, mut filter: F)
    where
        F: FnMut(Line) -> bool,
    {
        for index in (0..self.data.len()).rev() {
            let line = &self.data[index];
            let line = Line::new(&self.header, line);
            if filter(line).not() {
                self.data.swap_remove(index);
            } //else keep
        }
    }

    pub fn group_by<F, G>(mut self, mut grouper: F) -> Groups<G>
    where
        F: FnMut(Line) -> G,
        G: Hash + Eq,
    {
        let mut map = HashMap::new();
        self.data.drain(..).enumerate().for_each(|(i, raw_line)| {
            let line = Line::new(&self.header, &raw_line);
            let key = grouper(line);
            let vec: &mut Vec<_> = map.entry(key).or_default();
            vec.push((i, raw_line));
        });
        let mut map = map.drain().collect::<Vec<_>>();
        //sort so we do not get non determinismus by useing a Hashmap
        map.sort_by_key(|(_g, vec)| vec[0].0);
        let mut groups = Vec::new();
        for (key, mut raw_data) in map {
            let mut df = DataFrame::new(self.header.clone());
            df.append_lines(raw_data.drain(..).map(|(_, line)| line));
            groups.push((key, df));
        }
        Groups::new(groups)
    }

    pub fn header(&self) -> &Vec<String> {
        &self.header
    }

    pub fn iter(&self) -> impl Iterator<Item = Line> + DoubleEndedIterator + ExactSizeIterator {
        self.data.iter().map(|line| Line::new(&self.header, line))
    }
}

impl<'df> IntoIterator for &'df DataFrame {
    type Item = Line<'df>;

    type IntoIter = impl Iterator<Item = Line<'df>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Display for DataFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", View::new(self, ViewType::Full))
    }
}
