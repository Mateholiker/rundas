use std::fmt::{Display, Formatter, Result as FmtResult};

use super::{DataFrame, Line};

#[derive(Debug)]
pub struct View<'df> {
    df: &'df DataFrame,
    view_type: ViewType,
}

impl<'df> View<'df> {
    pub(super) fn new(df: &'df DataFrame, view_type: ViewType) -> View<'df> {
        View { df, view_type }
    }

    fn print_lines(
        &self,
        f: &mut Formatter<'_>,
        lines: impl Iterator<Item = (usize, Line<'df>)>,
    ) -> FmtResult {
        let mut print_table = vec![vec!["#".into()]];

        for head_elem in self.df.header() {
            print_table.push(vec![format!("{}", head_elem)]);
        }

        for (line_number, line) in lines {
            print_table[0].push(format!("{}", line_number));
            for (i, elem) in line.iter().enumerate() {
                print_table[i + 1].push(format!("{}", elem));
            }
        }

        let mut max_width = Vec::new();
        for row in print_table.iter() {
            max_width.push(
                row.iter()
                    .map(|string| string.chars().count())
                    .max()
                    .expect("unreachable"),
            );
        }

        for line_index in 0..print_table[0].len() {
            for (row_index, row) in print_table.iter().enumerate() {
                write!(
                    f,
                    "{:<width$}",
                    row[line_index],
                    width = max_width[row_index] + 2
                )?;
            }
            writeln!(f)?;
        }

        Ok(())
    }
}

impl<'df> Display for View<'df> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        use ViewType::{Full, Head, Range, Tail};
        match self.view_type {
            Head(lines) => self.print_lines(f, self.df.iter().enumerate().take(lines)),

            Tail(lines) => self.print_lines(f, self.df.iter().enumerate().rev().take(lines).rev()),

            Range(start, end) => {
                self.print_lines(f, self.df.iter().enumerate().skip(start).take(end - start))
            }

            Full => self.print_lines(f, self.df.iter().enumerate()),
        }
    }
}

#[derive(Debug)]
pub(super) enum ViewType {
    Head(usize),
    Tail(usize),
    Range(usize, usize),
    Full,
}
