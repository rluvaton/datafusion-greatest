pub(crate) trait Permutation: Clone {
    fn permutation(&self, len: usize) -> Vec<Self>;
}

impl<T: Clone + PartialEq> Permutation for Vec<T> {
    fn permutation(&self, len: usize) -> Vec<Self> {
        let mut result = vec![];

        if len == 0 {
            return result;
        }

        let mut stack = vec![(0, vec![])];

        while !stack.is_empty() {
            let (index, permutation) = stack.pop().unwrap();

            if index == len {
                result.push(permutation);
                continue;
            }

            for i in 0..self.len() {
                if !permutation.contains(&self[i]) {
                    let mut new_permutation = permutation.clone();
                    new_permutation.push(self[i].clone());

                    stack.push((index + 1, new_permutation));
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::Permutation;

    #[test]
    fn permutation() {
        let vec = vec![1, 2, 3];
        let mut expected = vec![
            vec![1, 2, 3],
            vec![1, 3, 2],
            vec![2, 1, 3],
            vec![2, 3, 1],
            vec![3, 1, 2],
            vec![3, 2, 1]
        ];

        let mut actual = vec.permutation(3);

        expected.sort();
        actual.sort();

        assert_eq!(actual, expected);

        // Making sure the sort did not change the inner vector order
        assert_ne!(expected[0], expected[1]);
        assert_ne!(actual[0], actual[1]);
    }
}
