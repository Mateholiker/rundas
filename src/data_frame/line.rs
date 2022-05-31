use std::ops::Index;

use super::Data;

pub struct Line<'df> {
    header: &'df Vec<String>,
    line: &'df Vec<Data>,
    index_map: Vec<usize>,
}

impl<'df> IntoIterator for &Line<'df> {
    type Item = &'df Data;

    type IntoIter = impl Iterator<Item = &'df Data>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'df> Line<'df> {
    pub(super) fn new(header: &'df Vec<String>, line: &'df Vec<Data>) -> Line<'df> {
        Line {
            header,
            line,
            index_map: (0..line.len()).collect(),
        }
    }

    pub(super) fn with_index_map(mut self, index_map: Vec<usize>) -> Line<'df> {
        self.index_map = index_map;
        self
    }

    pub fn iter(&self) -> impl Iterator<Item = &'df Data> + '_ {
        self.index_map.iter().map(|index| &self.line[*index])
    }

    pub fn header(&self) -> impl Iterator<Item = &'df str> + '_ {
        self.index_map.iter().map(|index| &self.header[*index][..])
    }
}

impl<'df> Index<usize> for Line<'df> {
    type Output = Data;

    fn index(&self, index: usize) -> &Self::Output {
        let index = self.index_map[index];
        &self.line[index]
    }
}

impl<'df> Index<&str> for Line<'df> {
    type Output = Data;

    fn index(&self, index: &str) -> &Self::Output {
        if let Some((index, _)) = self
            .header()
            .enumerate()
            .find(|(_i, string)| index == *string)
        {
            &self[index]
        } else {
            panic!(
                "index out of Bound header is {:?} but index was '{}'",
                self.header().collect::<Vec<_>>(),
                index
            )
        }
    }
}
