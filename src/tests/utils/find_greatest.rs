use std::any::{Any, TypeId};
use crate::helpers::Transpose;

/// Results are returned as a matrix where each row corresponds to a column in the DataFrame.
pub(crate) fn find_greatest<T: PartialOrd + Copy + 'static>(results: Vec<Vec<T>>) -> Vec<T> {
    let rows = results.transpose();

    rows.iter().map(|row| {
        let max: T = row[0];

        row.iter().fold(max, |acc, &x| {
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

}

