#[cfg(test)]
mod scalars_and_arrays_tests {
    use crate::helpers::Permutation;
    use crate::tests::utils::{create_context, create_primitive_array, find_greatest, get_primitive_result_as_matrix};
    use datafusion::arrow::array::{ArrayRef, RecordBatch};
    use datafusion::arrow::datatypes::Int8Type;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{col, lit};

    #[tokio::test]
    async fn i8_without_nulls_and_single_scalar() {
        let (ctx, greatest) = create_context();

        let a_vec = vec![Some(1), Some(4), Some(-8), Some(126)];
        let b_vec = vec![Some(3), Some(-23), Some(34), Some(3)];
        let a: ArrayRef = create_primitive_array::<Int8Type>(a_vec.clone());
        let b: ArrayRef = create_primitive_array::<Int8Type>(b_vec.clone());

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        let df = ctx.table("t").await.unwrap();

        let greatest_args = vec![col("a"), col("b"), lit(5i8)];

        for args in greatest_args.permutation(greatest_args.len()) {
            let df = df.clone().select(vec![
                greatest.call(args)
            ]).unwrap();

            let results = get_primitive_result_as_matrix::<Int8Type>(df).await.unwrap();

            assert_eq!(results, vec![
                find_greatest(vec![a_vec.clone(), b_vec.clone(), vec![Some(5i8); a_vec.len()]])
            ]);
        }
    }

    #[tokio::test]
    async fn i8_without_nulls_and_multiple_scalars() {
        let (ctx, greatest) = create_context();

        let a_vec = vec![Some(1), Some(4), Some(-8), Some(126), None, Some(5)];
        let b_vec = vec![Some(3), Some(-23), Some(34), Some(3), None, Some(5)];
        let a: ArrayRef = create_primitive_array::<Int8Type>(a_vec.clone());
        let b: ArrayRef = create_primitive_array::<Int8Type>(b_vec.clone());

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        let df = ctx.table("t").await.unwrap();

        let greatest_args = vec![col("a"), col("b"), lit(5i8), lit(7i8), lit(-10i8), lit(ScalarValue::Null)];

        for args in greatest_args.permutation(greatest_args.len()) {
            let df = df.clone().select(vec![
                greatest.call(args)
            ]).unwrap();

            let results = get_primitive_result_as_matrix::<Int8Type>(df).await.unwrap();

            assert_eq!(results, vec![
                find_greatest(vec![a_vec.clone(), b_vec.clone(), vec![Some(7i8); a_vec.len()]])
            ]);
        }
    }

}
