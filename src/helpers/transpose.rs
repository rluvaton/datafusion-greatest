pub(crate) trait Transpose {
    fn transpose(&self) -> Self;
}

impl<T: Clone> Transpose for Vec<Vec<T>> {
    fn transpose(&self) -> Self {
        let mut rows = vec![];

        for i in 0..self.len() {
            for j in 0..self[i].len() {
                if rows.len() <= j {
                    rows.push(vec![]);
                }

                // This does not suppose to be fast as it's just for tests
                rows[j].push(self[i][j].clone());
            }
        }

        rows
    }
}

#[cfg(test)]
mod tests {
    use super::Transpose;

    #[test]
    fn transpose() {
        let matrix = vec![
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![7, 8, 9]
        ];

        let expected = vec![
            vec![1, 4, 7],
            vec![2, 5, 8],
            vec![3, 6, 9]
        ];

        assert_eq!(matrix.transpose(), expected);
    }
}



