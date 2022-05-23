use crate::{Data, DataFrame};

impl DataFrame {
    pub fn append_line(&mut self, line: Vec<Data>) {
        assert_eq!(self.header().len(), line.len());
        self.data.push(line);
    }

    pub fn append_lines(&mut self, lines: impl Iterator<Item = Vec<Data>>) {
        lines.for_each(|line| self.append_line(line));
    }

    pub fn append_data_frame(&mut self, mut other: DataFrame) {
        assert_eq!(self.header, other.header);
        self.data.append(&mut other.data);
    }
}
