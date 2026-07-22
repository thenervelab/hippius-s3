//! A minimal fixed-width ASCII table renderer for the decision-demo suites.
//!
//! Hand-rolled rather than pulled from a crate: the demos need exactly one
//! thing — align columns so a reader can scan the decision on each row — and a
//! table dependency would be more attack surface and build cost than the few
//! lines below justify.

/// An owned, render-once table: a title, a header row, and body rows.
///
/// Owns every string because each demo builds a throwaway table from local
/// scenario data; borrowing would only complicate the call sites for no gain.
#[derive(Debug)]
pub(crate) struct Table {
    title: String,
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl Table {
    /// Starts a table titled `title` with the given column `headers`.
    #[must_use]
    pub(crate) fn new(title: impl Into<String>, headers: &[&str]) -> Self {
        Self {
            title: title.into(),
            headers: headers.iter().map(|h| (*h).to_owned()).collect(),
            rows: Vec::new(),
        }
    }

    /// Appends one body row.
    ///
    /// A cell count that differs from the header count is a test-authoring bug,
    /// so it trips an assertion loudly rather than rendering a skewed table.
    pub(crate) fn row(&mut self, cells: &[String]) {
        assert_eq!(cells.len(), self.headers.len(), "row width must match the header width");
        self.rows.push(cells.to_vec());
    }

    /// Renders to a fixed-width string: title, header, an underline rule, then
    /// the rows. Every column is left-padded to its widest cell (header
    /// included), so all column-bearing lines share one display width.
    ///
    /// Pure: the same table always renders the same string.
    #[must_use]
    pub(crate) fn render(&self) -> String {
        // Column width = widest cell in that column, header included. Seed from
        // the headers, then widen for every body cell.
        let mut widths: Vec<usize> = self.headers.iter().map(|h| h.chars().count()).collect();
        for row in &self.rows {
            for (col, cell) in row.iter().enumerate() {
                widths[col] = widths[col].max(cell.chars().count());
            }
        }

        let line = |cells: &[String]| -> String {
            cells
                .iter()
                .enumerate()
                .map(|(col, cell)| format!("{cell:<width$}", width = widths[col]))
                .collect::<Vec<_>>()
                .join("  ")
        };
        let rule: String = widths.iter().map(|w| "-".repeat(*w)).collect::<Vec<_>>().join("  ");

        let mut out = String::new();
        out.push_str(&self.title);
        out.push('\n');
        out.push_str(&line(&self.headers));
        out.push('\n');
        out.push_str(&rule);
        for row in &self.rows {
            out.push('\n');
            out.push_str(&line(row));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::Table;

    #[test]
    fn render_aligns_columns_to_the_widest_cell() {
        let mut table = Table::new("demo", &["a", "bbb"]);
        table.row(&["xx".to_owned(), "y".to_owned()]);
        let out = table.render();

        // Skip the title line; every remaining line (header, rule, body) must be
        // the same display width once columns are padded to their widest cell.
        let widths: Vec<usize> = out.lines().skip(1).map(|line| line.chars().count()).collect();
        assert!(!widths.is_empty(), "render must produce a header, a rule, and body lines");
        assert!(
            widths.windows(2).all(|pair| pair[0] == pair[1]),
            "all column-bearing lines must share one width, got {widths:?}",
        );
    }
}
