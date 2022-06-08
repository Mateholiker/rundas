use super::{BaseDataFrame, Data, DataFrame, InnerDataFrame};

impl DataFrame {
    pub fn append_line(self, mut line: Vec<Data>) -> DataFrame {
        let mut base = BaseDataFrame::from(self);
        base.append_line(line.drain(..));
        InnerDataFrame::Base { df: base }.into()
    }

    pub fn append_lines<'odf>(
        self,
        lines: impl Iterator<Item = impl Iterator<Item = Data<'odf>>>,
    ) -> DataFrame {
        let mut base = BaseDataFrame::from(self);
        base.append_lines(lines);
        InnerDataFrame::Base { df: base }.into()
    }

    pub fn append_data_frame(self, other: &DataFrame) -> DataFrame {
        let mut base = BaseDataFrame::from(self);
        base.append_data_frame(other);
        InnerDataFrame::Base { df: base }.into()
    }
}

impl BaseDataFrame {
    pub fn append_line<'odf>(&mut self, line: impl Iterator<Item = Data<'odf>>) {
        let line: Vec<_> = line
            .map(|data| data.into_inner_data(&mut self.string_storage))
            .collect();
        assert_eq!(self.header.len(), line.len());
        self.data.push(line);
    }

    pub fn append_lines<'df>(
        &mut self,
        lines: impl Iterator<Item = impl Iterator<Item = Data<'df>>>,
    ) {
        lines.for_each(|line| self.append_line(line));
    }

    pub fn append_data_frame(&mut self, other: &DataFrame) {
        assert!(self.has_same_header(other));
        self.append_lines(other.iter().map(|line| line.iter()));
    }

    fn has_same_header(&self, other: &DataFrame) -> bool {
        let mut self_header_iter = self.header.iter();
        let mut other_header_iter = other.header();
        loop {
            match (self_header_iter.next(), other_header_iter.next()) {
                (Some(range), Some(s2)) => {
                    let s1 = self
                        .string_storage
                        .get(range.clone())
                        .expect("Header index inconsitant with string_storage UTF8 boundary");
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
