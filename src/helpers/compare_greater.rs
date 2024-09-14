use datafusion::arrow::array::{make_comparator, Array, ArrayRef, BooleanArray};
use datafusion::arrow::compute::SortOptions;
use datafusion::error::Result;

use datafusion::arrow::compute::kernels::cmp;
use datafusion::arrow::compute::kernels::zip::zip;

/// Return boolean array where `arr[i] = lhs[i] >= rhs[i]` for all i, where `arr` is the result array
/// Nulls are always considered smaller than any other value
pub(crate) fn get_larger(lhs: &dyn Array, rhs: &dyn Array) -> Result<BooleanArray> {

    // Fast path:
    // If both arrays are not nested, have the same length and no nulls, we can use the faster vectorised kernel
    // - If both arrays are not nested: Nested types, such as lists, are not supported as the null semantics are not well-defined.
    // - both array does not have any nulls: cmp::gt_eq will return null if any of the input is null while we want to return false in that case
    if !lhs.data_type().is_nested() && lhs.null_count() == 0 && rhs.null_count() == 0 {
        return cmp::gt_eq(&lhs, &rhs).map_err(|e| e.into()); // Use faster vectorised kernel
    }

    let sort_options = SortOptions {
        // We want greatest first
        descending: false,

        // NULL will be less than any other value
        nulls_first: true,
    };

    let cmp = make_comparator(lhs, rhs, sort_options)?;

    // TODO - handle rest of length
    let len = lhs.len().min(rhs.len());

    // TODO - avoid this allocation
    let values = (0..len).map(|i| cmp(i, i).is_ge()).collect();

    // No nulls as we only want to keep the values that are larger, its either true or false
    Ok(BooleanArray::new(values, None))
}

/// Return array where the largest value at each index is kept
pub(crate) fn keep_larger(lhs: ArrayRef, rhs: ArrayRef) -> Result<ArrayRef> {

    // If one of the array is all null, return the other array

    if rhs.null_count() == rhs.len() {
        return Ok(lhs);
    }

    if lhs.null_count() == lhs.len() {
        return Ok(rhs.clone());
    }

    // True for values that we should keep from the left array
    let keep_lhs = get_larger(lhs.as_ref(), rhs.as_ref())?;

    let larger = zip(&keep_lhs, &lhs, &rhs)?;

    Ok(larger)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::utils::create_primitive_array;
    use crate::*;
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::Int8Type;

    fn keep_larger_for_regular_vecs<T: Ord + Copy>(vectors: Vec<Vec<Option<T>>>) -> Vec<Option<T>> {
        vectors
            .transpose()
            .iter()
            .map(|v| {
                let mut max = v[0];
                for x in v {
                    if max.is_none() {
                        max = *x;
                    } else if x.is_none() {
                        continue;
                    } else if x > &max {
                        max = *x;
                    }
                }
                max
            })
            .collect()
    }

    #[test]
    fn should_return_ready_when_primitive_with_nulls() {
        let vec1 = vec![Some(-1), None, Some(3), Some(0), None, None, Some(7), Some(8), Some(9), Some(10)];
        let vec2 = vec![Some(5), None, Some(-2), None, Some(4), Some(20), None, Some(8), Some(12), Some(-3)];
        let expected = keep_larger_for_regular_vecs(vec![vec1.clone(), vec2.clone()]);

        let array1 = create_primitive_array::<Int8Type>(vec1.clone());
        let array2 = create_primitive_array::<Int8Type>(vec2.clone());

        let result = keep_larger(array1, array2).unwrap();

        assert_eq!(result.as_primitive::<Int8Type>().clone(), *create_primitive_array::<Int8Type>(expected).as_ref());
    }

    #[test]
    fn should_return_ready_when_primitives_without_nulls() {
        let vec1 = vec![Some(-1), Some(9), Some(3), Some(0), Some(11), Some(-120), Some(7), Some(8), Some(9), Some(10)];
        let vec2 = vec![Some(5), Some(13), Some(-2), Some(3), Some(4), Some(20), Some(83), Some(8), Some(12), Some(-3)];
        let expected = keep_larger_for_regular_vecs(vec![vec1.clone(), vec2.clone()]);

        let array1 = create_primitive_array::<Int8Type>(vec1.clone());
        let array2 = create_primitive_array::<Int8Type>(vec2.clone());

        let result = keep_larger(array1, array2).unwrap();

        assert_eq!(result.as_primitive::<Int8Type>().clone(), *create_primitive_array::<Int8Type>(expected).as_ref());
    }
}
