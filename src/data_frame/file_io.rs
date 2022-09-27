use std::fmt::Write;

use super::{BaseDataFrame, Data, DataFrame, InnerDataFrame};
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

    pub fn from_string(string: String, seperator: Option<char>) -> Result<DataFrame, IoError> {
        let base = BaseDataFrame::from_string(string, seperator)?;
        Ok(InnerDataFrame::Base { df: base }.into())
    }
}

impl BaseDataFrame {
    fn from_file(path: &Path, seperator: Option<char>) -> Result<BaseDataFrame, IoError> {
        let seperator = seperator.unwrap_or(',');
        let file = File::open(&path)?;
        let reader = BufReader::new(file);

        let mut line_iter = reader.lines().enumerate();

        let (_i, raw_header) = line_iter
            .next()
            .ok_or_else(|| IoError::other("File is empty"))?;

        //trim an invisible char thats exel adds as an encoding hint
        let raw_header = raw_header.map(|string| string.trim_matches('\u{feff}').to_owned());
        let header = BaseDataFrame::try_build_header(ChunkIter::from_str(&raw_header?, seperator))?;

        let data = BaseDataFrame::get_data_from_file(&header, line_iter, seperator)?;

        Ok(BaseDataFrame {
            identity_index_map: (0..header.len()).collect(),
            header,
            data,
        })
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

        let line_iter = reader
            .lines()
            .enumerate()
            .skip(if skip_first_line { 1 } else { 0 });
        let mut data = BaseDataFrame::get_data_from_file(&self.header, line_iter, seperator)?;
        self.append_lines(data.drain(..));
        Ok(())
    }

    fn from_string(string: String, seperator: Option<char>) -> Result<BaseDataFrame, IoError> {
        let seperator = seperator.unwrap_or(',');
        let mut line_iter = string.lines().enumerate();
        let (_i, raw_header) = line_iter
            .next()
            .ok_or_else(|| IoError::other("String is empty"))?;
        let header = BaseDataFrame::try_build_header(ChunkIter::from_str(raw_header, seperator))?;

        let mut data = Vec::new();
        for (i, line) in line_iter {
            let chunk_iter = ChunkIter::from_str(line, seperator);
            let line_data: Vec<Data> = chunk_iter.collect();

            if line_data.len() != header.len() {
                return Err(Self::create_error(i, &line_data, &header));
            }
            data.push(line_data);
        }

        Ok(BaseDataFrame {
            identity_index_map: (0..header.len()).collect(),
            header,
            data,
        })
    }

    fn get_data_from_file(
        header: &[String],
        line_iter: impl Iterator<Item = (usize, Result<String, IoError>)>,
        seperator: char,
    ) -> Result<Vec<Vec<Data>>, IoError> {
        let mut data = Vec::new();
        for (i, line_res) in line_iter {
            let line = line_res?;
            let chunk_iter = ChunkIter::from_str(&line, seperator);
            let line_data: Vec<Data> = chunk_iter.collect();

            if line_data.len() != header.len() {
                return Err(Self::create_error(i, &line_data, header));
            }
            data.push(line_data);
        }
        Ok(data)
    }

    fn try_build_header(raw_header: ChunkIter) -> Result<Vec<String>, IoError> {
        let mut header = Vec::new();
        for data in raw_header {
            if let Data::String(string) = data {
                header.push(Box::<String>::into_inner(string));
            } else {
                return Err(IoError::other("File has no valid Header"));
            }
        }
        Ok(header)
    }

    fn create_error(line_index: usize, line_data: &[Data], header: &[String]) -> IoError {
        let id = if line_data.len() > header.len() {
            "more"
        } else {
            "less"
        };
        let mut header_iter = header.iter().peekable();
        let mut line_iter = line_data.iter().peekable();
        let mut pairs: Vec<(Option<String>, Option<Data>)> = Vec::new();
        while let (Some(header_elem), Some(line_elem)) = (header_iter.peek(), line_iter.peek()) {
            pairs.push((Some(header_elem.to_string()), Some((*line_elem).clone())));
            header_iter.next();
            line_iter.next();
        }
        for elem in header_iter {
            pairs.push((Some(elem.to_string()), None));
        }
        for elem in line_iter {
            pairs.push((None, Some(elem.clone())));
        }
        IoError::other({
            let mut header_string = format!(
                "Line {} contrains {} entries than the header; Line.len() = {}, Header.len() = {};\n",
                line_index + 1,
                id,
                line_data.len(),
                header.len()
            );
            for (h, l) in pairs {
                write!(header_string, "{}:  ", h.unwrap_or_else(|| "None".into()))
                    .expect("should be fine");
                if let Some(l) = l {
                    write!(header_string, "{:?}", l).expect("should be fine")
                } else {
                    header_string.push_str("None");
                }
                header_string.push('\n');
            }
            header_string
        })
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
    type Item = Data;

    fn next(&mut self) -> Option<Self::Item> {
        let trimed = self.string.trim_start();
        let mut chars = trimed.char_indices().peekable();

        if let Some(&(start_index, first)) = chars.peek() {
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

                let item = Data::Vector(Box::new(inner_iter.collect()));
                let trimed_end_index = self.string.ceil_char_boundary(end_index + 1);
                self.string = &self.string[trimed_end_index..];
                Some(item)
            } else if let Some((end_index, _seperator)) =
                chars.find(|(_index, elem)| *elem == self.seperator)
            {
                let item = Data::from(self.string[start_index..end_index].to_owned());
                let trimed_end_index = self.string.ceil_char_boundary(end_index + 1);
                self.string = &self.string[trimed_end_index..];
                Some(item)
            } else {
                let item = Data::from(self.string[start_index..].to_owned());
                self.string = &self.string[0..0];
                Some(item)
            }
        } else {
            None
        }
    }
}
