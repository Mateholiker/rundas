use std::fmt::{Display, Formatter, Result as FmtResult};

use super::DataFrame;

impl Display for DataFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let lines = self.iter().enumerate();
        let mut print_table = vec![vec!["#".into()]];

        for head_elem in self.header() {
            print_table.push(vec![format!("{head_elem}")]);
        }

        for (line_number, line) in lines {
            print_table[0].push(format!("{line_number}"));
            for (i, elem) in line.iter().enumerate() {
                print_table[i + 1].push(format!("{elem}"));
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
