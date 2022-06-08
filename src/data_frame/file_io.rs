use std::ops::{Deref, Range};

use crate::Data;

use super::{BaseDataFrame, DataFrame, InnerDataFrame};
use std::{
    fs::File,
    io::{BufRead, BufReader, Error as IoError},
    path::Path,
};

impl DataFrame {
    pub fn from_file(path: &Path, seperator: Option<char>) -> Result<DataFrame, IoError> {
        let base = BaseDataFrame::from_file(path, seperator)?;
        Ok(InnerDataFrame::Base { df: base }.into())
    }

    pub fn append_file(
        self,
        path: &Path,
        seperator: Option<char>,
        skip_first_line: bool,
    ) -> Result<DataFrame, IoError> {
        let mut base = BaseDataFrame::from(self);
        base.append_file(path, seperator, skip_first_line)?;
        Ok(InnerDataFrame::Base { df: base }.into())
    }
}

impl BaseDataFrame {
    fn from_file(path: &Path, seperator: Option<char>) -> Result<BaseDataFrame, IoError> {
        let seperator = seperator.unwrap_or(',');
        let file = File::open(&path)?;
        let reader = BufReader::new(file);

        let mut line_iter = reader.lines();
        let raw_header = line_iter
            .next()
            .ok_or_else(|| IoError::other("File is empty"))?;
        let (string_storage, header) =
            BaseDataFrame::try_build_header(ChunkIter::from_str(&raw_header?, seperator))?;

        let mut df = BaseDataFrame {
            string_storage,
            identity_index_map: (0..header.len()).collect(),
            header,
            data: Vec::new(),
        };

        df.append_file(path, Some(seperator), true)?;
        Ok(df)
    }

    fn try_build_header(raw_header: ChunkIter) -> Result<(String, Vec<Range<usize>>), IoError> {
        let mut string_storage = String::new();
        let mut header = Vec::new();
        for data in raw_header {
            if let Data::String(string) = data {
                let start = string_storage.len();
                string_storage.push_str(string.deref());
                let end = string_storage.len();
                header.push(start..end);
            } else {
                return Err(IoError::other("File has no valid Header"));
            }
        }
        Ok((string_storage, header))
    }

    fn append_file(
        &mut self,
        path: &Path,
        seperator: Option<char>,
        skip_first_line: bool,
    ) -> Result<(), IoError> {
        let seperator = seperator.unwrap_or(',');
        let file = File::open(&path)?;
        let reader = BufReader::new(file);

        let line_iter = reader.lines().skip(if skip_first_line { 1 } else { 0 });

        for line_string_result in line_iter {
            let line_string = line_string_result?;
            let chunk_iter = ChunkIter::from_str(&line_string, seperator);
            self.append_line(chunk_iter);
        }

        Ok(())
    }
}

const GROUPING_SYMBOLE: [(char, char); 6] = [
    ('(', ')'),
    ('{', '}'),
    ('<', '>'),
    ('[', ']'),
    ('"', '"'),
    ('\'', '\''),
];

struct ChunkIter<'s> {
    string: &'s str,
    seperator: char,
}

impl<'s> ChunkIter<'s> {
    fn from_str(string: &'s str, seperator: char) -> ChunkIter {
        ChunkIter { string, seperator }
    }
}

impl<'s> Iterator for ChunkIter<'s> {
    type Item = Data<'s>;

    fn next(&mut self) -> Option<Self::Item> {
        let trimed = self.string.trim_start();
        let mut chars = trimed.char_indices();

        if let Some((start_index, first)) = chars.next() {
            if let Some((_start, end)) = GROUPING_SYMBOLE
                .iter()
                .find(|(start, _end)| *start == first)
            {
                let (end_index, _end_symbole) = chars
                    .find(|(_index, elem)| elem == end)
                    .expect("Line contains a start but no matching end grouping symbole");

                let trimed_start_index = self.string.ceil_char_boundary(start_index + 1);
                let inner_iter = ChunkIter::from_str(
                    &self.string[trimed_start_index..end_index],
                    self.seperator,
                );

                let item = Data::Vector(inner_iter.collect());
                let trimed_end_index = self.string.ceil_char_boundary(end_index + 1);
                self.string = &self.string[trimed_end_index..];
                Some(item)
            } else if let Some((end_index, _seperator)) =
                chars.find(|(_index, elem)| *elem == self.seperator)
            {
                let item = Data::from(&self.string[start_index..end_index]);
                let trimed_end_index = self.string.ceil_char_boundary(end_index + 1);
                self.string = &self.string[trimed_end_index..];
                Some(item)
            } else {
                let item = Data::from(&self.string[start_index..]);
                self.string = &self.string[0..0];
                Some(item)
            }
        } else {
            None
        }
    }
}
