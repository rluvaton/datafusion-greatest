use crate::helpers::Transpose;

/// Results are returned as a matrix where each row corresponds to a column in the DataFrame.
pub(crate) fn find_greatest<T: PartialOrd + Clone + 'static>(results: Vec<Vec<T>>) -> Vec<T> {
    let rows = results.transpose();

    rows.iter().map(|row| {
        let max: T = row[0].clone();

        row.iter().fold(max, |acc, x| {
            let x = x.clone();
            if x > acc {
                x
            } else {
                acc
            }
        })
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_expected_greatest_i8() {
        let col_a: Vec<i8> = vec![
            1,
            4,
            -8,
            126
        ];
        let col_b: Vec<i8> = vec![
            3,
            -23,
            34,
            3
        ];
        let col_c: Vec<i8> = vec![
            -100,
            9,
            0,
            4
        ];
        let cols = vec![col_a, col_b, col_c];

        assert_eq!(find_greatest(cols), vec![
            3,
            9,
            34,
            126
        ]);
    }

    #[test]
    fn test_get_expected_greatest_i16() {
        let col_a: Vec<i16> = vec![
            1,
            4,
            -8,
            126
        ];
        let col_b: Vec<i16> = vec![
            3,
            -23,
            34,
            3
        ];
        let col_c: Vec<i16> = vec![
            -100,
            9,
            0,
            4
        ];
        let cols = vec![col_a, col_b, col_c];

        assert_eq!(find_greatest(cols), vec![
            3,
            9,
            34,
            126
        ]);
    }

    #[test]
    fn test_get_expected_greatest_i8_2_rows() {
        let col_a: Vec<i8> = vec![117, 56, -120, 115, 68, -95, -94, -96];
        let col_b: Vec<i8> = vec![-114, -100, -37, 11, 109, -4, -67, 29];
        let cols = vec![col_a, col_b];
        let expected = vec![117, 56, -37, 115, 109, -4, -67, 29];

        assert_eq!(find_greatest(cols), expected);
    }

    #[test]
    fn test_get_expected_greatest_nullable_i8() {
        let col_a: Vec<Option<i8>> = vec![Some(1), None, Some(-10), Some(4), None, Some(120), Some(7), Some(30)];
        let col_b: Vec<Option<i8>> = vec![Some(5), None, None, Some(-2), Some(10), Some(1), Some(23), None];
        let cols = vec![col_a, col_b];
        let expected = vec![Some(5), None, Some(-10), Some(4), Some(10), Some(120), Some(23), Some(30)];

        assert_eq!(find_greatest(cols), expected);
    }

    #[test]
    fn test_get_expected_greatest_nullable_f32() {
        let col_a: Vec<Option<f32>> = vec![Some(1f32), None, Some(f32::INFINITY), Some(f32::NAN), Some(f32::NAN)];
        let col_b: Vec<Option<f32>> = vec![Some(5f32), None, Some(f32::NEG_INFINITY), Some(0f32), Some(f32::INFINITY)];
        let cols = vec![col_a, col_b];

        let mut actual = find_greatest(cols);

        assert_eq!(actual.remove(0), Some(5f32));
        assert_eq!(actual.remove(0), None);
        assert_eq!(actual.remove(0), Some(f32::INFINITY));

        // NaN is always the greatest
        assert_eq!(actual.remove(0).unwrap().is_nan(), true);

        // NaN is always the greatest, even if there is infinity
        assert_eq!(actual.remove(0).unwrap().is_nan(), true);

        assert_eq!(actual, vec![]);
    }

    #[test]
    fn test_strings() {
        let a_vec = vec![Some("AR".to_string()), None, Some("BS".to_string()), Some("CL".to_string())];
        let b_vec = vec![Some("LA".to_string()), Some("BC".to_string()), None, Some("D2".to_string())];

        let mut actual = find_greatest(vec![a_vec, b_vec]);

        assert_eq!(actual.remove(0), Some("LA".to_string()));
        assert_eq!(actual.remove(0), Some("BC".to_string()));
        assert_eq!(actual.remove(0), Some("BS".to_string()));
        assert_eq!(actual.remove(0), Some("D2".to_string()));

        assert_eq!(actual, vec![]);
    }

    #[test]
    fn test_lists() {
        let rows = vec![
            vec![
                // Greatest is 2 as we look at the first item in each list first
                Some(vec![Some(2), Some(100)]),
                Some(vec![Some(1), Some(200)]),
            ],
            vec![
                // Greatest is 1 as 1 is greater than None
                Some(vec![None, Some(100)]),
                Some(vec![Some(1), Some(200)]),
            ],
            vec![
                // Greatest is 200 as if the first item is equal we look at the second item
                Some(vec![Some(6), Some(100)]),
                Some(vec![Some(6), Some(200)]),
            ],
            vec![
                // Greatest is 200 as if the first item is equal we look at the second item
                Some(vec![None, Some(100)]),
                Some(vec![None, Some(200)]),
            ],
            vec![
                // Greatest is None as having a value is greater than no value
                Some(vec![None]),
                Some(vec![]),
            ],
            vec![
                // Greatest is 1 as 1 is greater than 0, even though the length is different
                Some(vec![Some(1)]),
                Some(vec![Some(0), Some(4)]),
            ],
            vec![
                // Greatest is 4 as 4 is greater than nothing
                Some(vec![Some(0)]),
                Some(vec![Some(0), Some(4)]),
            ],
            vec![
                // Greatest is 0 as 0 is greater than None
                Some(vec![None, Some(3)]),
                Some(vec![Some(0)]),
            ],
        ];

        let actual = find_greatest(
            // Need to get as column
            rows.transpose()
        );

        assert_eq!(actual, vec![
            Some(vec![Some(2), Some(100)]),
            Some(vec![Some(1), Some(200)]),
            Some(vec![Some(6), Some(200)]),
            Some(vec![None, Some(200)]),
            Some(vec![None]),
            Some(vec![Some(1)]),
            Some(vec![Some(0), Some(4)]),
            Some(vec![Some(0)]),
        ]);
    }

}

