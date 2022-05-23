use std::ops::Index;

use super::Data;

pub struct Line<'df> {
    header: &'df Vec<String>,
    line: &'df Vec<Data>,
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
        Line { header, line }
    }

    pub fn iter(&self) -> impl Iterator<Item = &'df Data> {
        self.line.iter()
    }

    pub fn header(&self) -> &Vec<String> {
        self.header
    }
}

impl<'df> Index<usize> for Line<'df> {
    type Output = Data;

    fn index(&self, index: usize) -> &Self::Output {
        &self.line[index]
    }
}

impl<'df> Index<&str> for Line<'df> {
    type Output = Data;

    fn index(&self, index: &str) -> &Self::Output {
        if let Some((index, _)) = self
            .header
            .iter()
            .enumerate()
            .find(|(_i, string)| &index == string)
        {
            &self[index]
        } else {
            panic!(
                "index out of Bound header is {:?} but index was '{}'",
                self.header, index
            )
        }
    }
}
