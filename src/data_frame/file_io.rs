use std::fmt::Write;

use std::{
    fs::File,
    io::{BufRead, BufReader, Error as IoError},
    path::Path,
};

use crate::{Data, DataFrame};

impl DataFrame {
    pub fn from_file(path: &Path, seperator: Option<char>) -> Result<DataFrame, IoError> {
        let seperator = seperator.unwrap_or(',');
        let file = File::open(&path)?;
        let reader = BufReader::new(file);

        let mut line_iter = reader.lines().enumerate();
        let (_i, raw_header) = line_iter
            .next()
            .ok_or_else(|| IoError::other("File is empty"))?;
        let header = DataFrame::try_build_header(ChunkIter::from_string(raw_header?, seperator))?;

        let data = DataFrame::get_data_from_file(&header, line_iter, seperator)?;

        Ok(DataFrame { header, data })
    }

    pub fn append_file(
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
        let mut data = DataFrame::get_data_from_file(&self.header, line_iter, seperator)?;
        self.append_lines(data.drain(..));
        Ok(())
    }

    fn get_data_from_file(
        header: &[String],
        line_iter: impl Iterator<Item = (usize, Result<String, IoError>)>,
        seperator: char,
    ) -> Result<Vec<Vec<Data>>, IoError> {
        let mut data = Vec::new();
        for (i, line_res) in line_iter {
            let line = line_res?;
            let chunk_iter = ChunkIter::from_string(line, seperator);
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

struct ChunkIter {
    string: String,
    seperator: char,
}

impl ChunkIter {
    fn from_string(string: String, seperator: char) -> ChunkIter {
        ChunkIter { string, seperator }
    }
}

impl Iterator for ChunkIter {
    type Item = Data;

    fn next(&mut self) -> Option<Self::Item> {
        let trimed = self.string.trim();
        let mut chars = trimed.chars().peekable();
        if let Some(first) = chars.peek() {
            if let Some((_start, end)) = GROUPING_SYMBOLE
                .iter()
                .find(|(start, _end)| *start == *first)
            {
                //pop the first elem since it is equal to start
                chars.next();
                let rest: String = chars
                    .clone()
                    .skip_while(|elem| *elem != *end)
                    .skip(1)
                    .collect();
                let trimed = rest.trim();
                assert!(trimed.starts_with(self.seperator) || trimed.is_empty());
                let chunk: String = chars.take_while(|elem| *elem != *end).collect();
                let inner_iterator = ChunkIter::from_string(chunk, self.seperator);
                self.string = rest.trim().chars().skip(1).collect();

                let data_vec = Box::new(inner_iterator.collect());

                Some(Data::Vector(data_vec))
            } else {
                let rest = chars
                    .clone()
                    .skip_while(|elem| *elem != self.seperator)
                    .skip(1)
                    .collect();
                let chunk: String = chars.take_while(|elem| *elem != self.seperator).collect();
                self.string = rest;
                Some(chunk.into())
            }
        } else {
            None
        }
    }
}
