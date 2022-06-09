use super::{BaseDataFrame, Data, DataFrame, InnerDataFrame};

impl DataFrame {
    pub fn append_line(self, line: Vec<Data>) -> DataFrame {
        let mut base = BaseDataFrame::from(self);
        base.append_line(line);
        InnerDataFrame::Base { df: base }.into()
    }

    pub fn append_lines(self, lines: impl Iterator<Item = Vec<Data>>) -> DataFrame {
        let mut base = BaseDataFrame::from(self);
        base.append_lines(lines);
        InnerDataFrame::Base { df: base }.into()
    }

    pub fn append_data_frame(self, other: DataFrame) -> DataFrame {
        let mut base = BaseDataFrame::from(self);
        base.append_data_frame(other);
        InnerDataFrame::Base { df: base }.into()
    }
}

impl BaseDataFrame {
    pub(super) fn append_line(&mut self, line: Vec<Data>) {
        assert_eq!(self.header.len(), line.len());
        self.data.push(line);
    }

    pub(super) fn append_lines(&mut self, lines: impl Iterator<Item = Vec<Data>>) {
        lines.for_each(|line| self.append_line(line));
    }

    pub(super) fn append_data_frame(&mut self, other: DataFrame) {
        assert!(self.has_same_header(&other));
        self.append_lines(BaseDataFrame::from(other).data.drain(..));
    }

    fn has_same_header(&self, other: &DataFrame) -> bool {
        let mut self_header_iter = self.header.iter();
        let mut other_header_iter = other.header();
        loop {
            match (self_header_iter.next(), other_header_iter.next()) {
                (Some(s1), Some(s2)) => {
                    if s1 != s2 {
                        break false;
                    }
                }

                (None, None) => {
                    break true;
                }

                _ => {
                    break false;
                }
            }
        }
    }
}
