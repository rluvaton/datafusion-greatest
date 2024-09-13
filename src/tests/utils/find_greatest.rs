use std::any::{Any, TypeId};
use crate::tests::utils::transpose::Transpose;

/// Results are returned as a matrix where each row corresponds to a column in the DataFrame.
pub(crate) fn find_greatest<T: Ord + Copy + 'static>(results: Vec<Vec<T>>) -> Vec<T> {
    let rows = results.transpose();

    if TypeId::of::<T>() == TypeId::of::<f64>() {
        // TODO - find prettier way to do this
        (&rows as &dyn Any).downcast_ref::<Vec<f64>>().cloned().iter().map(|row| {
            [row.clone(), vec![f64::NAN]].concat()
                .into_iter()
                .reduce(f64::max)
                .map(|v| (&v as &dyn Any).downcast_ref::<T>().unwrap().clone())
                .unwrap()
        }).collect::<Vec<T>>()
    } else if TypeId::of::<T>() == TypeId::of::<f32>() {
        // TODO - find prettier way to do this
        (&rows as &dyn Any).downcast_ref::<Vec<f32>>().cloned().iter().map(|row| {
            [row.clone(), vec![f32::NAN]].concat()
                .into_iter()
                .reduce(f32::max)
                .map(|v| (&v as &dyn Any).downcast_ref::<T>().unwrap().clone())
                .unwrap()
        }).collect::<Vec<T>>()
    } else {
        rows.iter().map(|row| *row.iter().max().unwrap()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let expected = vec![7, 8, 9];

        assert_eq!(find_greatest(cols), vec![
            3,
            9,
            34,
            126
        ]);
    }

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
        let expected = vec![7, 8, 9];

        assert_eq!(find_greatest(cols), vec![
            3,
            9,
            34,
            126
        ]);
    }

    // TODO - add test for f32 and f64
}

