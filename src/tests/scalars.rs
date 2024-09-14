#[cfg(test)]
mod scalars_tests {
    /// These tests check that calling greatest only on scalars values
    ///
    /// This does not include columns
    ///

    use crate::tests::utils::{create_context, create_empty_data_frame, get_combined_results, get_result_as_matrix, parse_single_column};
    use datafusion::arrow::datatypes::{DataType, Float32Type, Float64Type, Int64Type, Int8Type};
    use datafusion_common::ScalarValue;
    use datafusion_expr::lit;
    use crate::helpers::{Permutation, Transpose};

    #[tokio::test]
    async fn i8_without_nulls() {
        let (ctx, greatest) = create_context();

        let df = create_empty_data_frame(&ctx, true).unwrap();

        let df = df.select(vec![greatest.call(vec![lit(2i8), lit(10i8)])]).unwrap();

        let results = get_result_as_matrix::<Int8Type>(df).await.unwrap();

        assert_eq!(results, vec![
            vec![Some(10)]
        ]);
    }

    #[tokio::test]
    async fn all_null() {
        let (ctx, greatest) = create_context();

        let df = create_empty_data_frame(&ctx, true).unwrap();

        let df = df.select(vec![greatest.call(vec![lit(ScalarValue::Null), lit(ScalarValue::Null), lit(ScalarValue::Null)])]).unwrap();

        let results = get_combined_results(df).await.unwrap();

        let results_data_type = results.columns().iter().map(|col| col.data_type()).cloned().collect::<Vec<_>>();

        assert_eq!(results_data_type, vec![DataType::Null]);
    }

    #[tokio::test]
    async fn i8_tests() {
        let (ctx, greatest) = create_context();

        let df = create_empty_data_frame(&ctx, true).unwrap();

        // Only integers - 1, 2, 3 => 3
        let only_integers_permutations = vec![lit(1i8), lit(2i8), lit(3i8)].permutation(3);

        // 1 None, 2 integers - 1, None, 5 => 5
        let two_integers_one_none_permutations = vec![lit(1i8), lit(ScalarValue::Null), lit(5i8)].permutation(3);

        // 2 None, 1 integer - None, None, 1 => 1
        let two_none_one_integer_permutations = vec![lit(ScalarValue::Null), lit(ScalarValue::Null), lit(1i8)].permutation(3);

        let df = df.select(vec![
            only_integers_permutations.iter().map(|v| greatest.call(v.clone())).collect::<Vec<_>>(),
            two_integers_one_none_permutations.iter().map(|v| greatest.call(v.clone())).collect::<Vec<_>>(),
            two_none_one_integer_permutations.iter().map(|v| greatest.call(v.clone())).collect::<Vec<_>>(),
        ].concat()).unwrap();

        let results = get_result_as_matrix::<Int8Type>(df).await.unwrap().transpose();

        assert_eq!(results, vec![[
            vec![Some(3); only_integers_permutations.len()],
            vec![Some(5); two_integers_one_none_permutations.len()],
            vec![Some(1); two_none_one_integer_permutations.len()],
        ].concat()]);
    }

    #[tokio::test]
    async fn f32_tests() {

        let (ctx, greatest) = create_context();

        let df = create_empty_data_frame(&ctx, true).unwrap();

        // Only f32 - 1, 2, 3 => 3
        let only_f32_permutations = vec![lit(1f32), lit(2f32), lit(3f32)].permutation(3);

        // 1 None, 2 f32 - 1, None, 5 => 5
        let two_f32_one_none_permutations = vec![lit(1f32), lit(ScalarValue::Null), lit(5f32)].permutation(3);

        // 2 None, 1 f32 - None, None, 1 => 1
        let two_none_one_f32_permutations = vec![lit(ScalarValue::Null), lit(ScalarValue::Null), lit(1f32)].permutation(3);

        let df = df.select(vec![
            only_f32_permutations.iter().map(|v| greatest.call(v.clone())).collect::<Vec<_>>(),
            two_f32_one_none_permutations.iter().map(|v| greatest.call(v.clone())).collect::<Vec<_>>(),
            two_none_one_f32_permutations.iter().map(|v| greatest.call(v.clone())).collect::<Vec<_>>(),
        ].concat()).unwrap();

        let results = get_result_as_matrix::<Float32Type>(df).await.unwrap().transpose();

        assert_eq!(results, vec![[
            vec![Some(3f32); only_f32_permutations.len()],
            vec![Some(5f32); two_f32_one_none_permutations.len()],
            vec![Some(1f32); two_none_one_f32_permutations.len()],
        ].concat()]);
    }

    #[tokio::test]
    async fn float_nan_tests() {

        let (ctx, greatest) = create_context();

        let df = create_empty_data_frame(&ctx, true).unwrap();

        // 1 None, 1 f32:NAN  and one regular f32
        let one_none_one_nan_one_number = vec![lit(1f32), lit(ScalarValue::Null), lit(f32::NAN)].permutation(3);

        let df = df.select(vec![
            one_none_one_nan_one_number.iter().map(|v| greatest.call(v.clone())).collect::<Vec<_>>(),
        ].concat()).unwrap();

        let results = get_result_as_matrix::<Float32Type>(df).await.unwrap().transpose();

        for result in results {
            for r in result {
                assert_eq!(r.is_none(), false);
                assert_eq!(r.is_some(), true);
                assert_eq!(r.unwrap().is_nan(), true);
            }
        }
    }

    #[tokio::test]
    async fn float_inf_tests() {

        let (ctx, greatest) = create_context();

        let df = create_empty_data_frame(&ctx, true).unwrap();

        // 1 None, 1 f32:NAN  and one regular f32
        let one_none_one_nan_one_number = vec![lit(1f32), lit(f32::INFINITY), lit(f32::NAN)].permutation(3);

        let df = df.select(vec![
            one_none_one_nan_one_number.iter().map(|v| greatest.call(v.clone())).collect::<Vec<_>>(),
        ].concat()).unwrap();

        let results = get_result_as_matrix::<Float32Type>(df).await.unwrap().transpose();

        for result in results {
            for r in result {
                assert_eq!(r.is_none(), false);
                assert_eq!(r.is_some(), true);
                assert_eq!(r.unwrap().is_nan(), true);
            }
        }
    }

    #[tokio::test]
    async fn multiple_types() {
        let (ctx, greatest) = create_context();

        let df = create_empty_data_frame(&ctx, true).unwrap();

        let df = df.select(vec![

            // i8, i16, i32, i64 all in the bounds of i8
            greatest.call(vec![lit(4i8), lit(2i16), lit(3i32), lit(-2i64)]),

            // i8, i16, i32, i64 all in the bounds of i16
            greatest.call(vec![lit(i8::MAX), lit(134i16), lit(i16::MAX as i32), lit(i16::MIN as i64)]),

            // i8, i16, i32, i64 all in the bounds of i32
            greatest.call(vec![lit(i8::MAX), lit(i16::MAX), lit(i32::MAX), lit(i32::MIN as i64)]),

            // i8, i16, i32, i64 all in the bounds of i64
            greatest.call(vec![lit(i8::MAX), lit(i16::MAX), lit(i32::MAX), lit(i64::MAX)]),

            // f32, i8, i16, i32, i64 all in the bounds of f32
            greatest.call(vec![lit(f32::MAX), lit(i8::MAX), lit(i16::MAX), lit(i32::MAX), lit(i64::MAX)]),

            // f32, f64, i8, i16, i32, i64 all in the bounds of f32
            greatest.call(vec![lit(43.342f32), lit(f32::MAX as f64), lit(i8::MAX), lit(i16::MAX), lit(i32::MAX), lit(i64::MAX)]),

            // f32, f64, i8, i16, i32, i64 all in the bounds of f64
            greatest.call(vec![lit(f32::MAX), lit(f64::MAX), lit(i8::MAX), lit(i16::MAX), lit(i32::MAX), lit(i64::MAX)]),

        ]).unwrap();

        let results = get_combined_results(df).await.unwrap();
        let mut columns: Vec<_> = results.columns().iter().collect();

        assert_eq!(parse_single_column::<Int64Type>(columns.remove(0)), vec![Some(4i64)]);
        assert_eq!(parse_single_column::<Int64Type>(columns.remove(0)), vec![Some(i16::MAX as i64)]);
        assert_eq!(parse_single_column::<Int64Type>(columns.remove(0)), vec![Some(i32::MAX as i64)]);
        assert_eq!(parse_single_column::<Int64Type>(columns.remove(0)), vec![Some(i64::MAX)]);
        assert_eq!(parse_single_column::<Float32Type>(columns.remove(0)), vec![Some(f32::MAX)]);
        assert_eq!(parse_single_column::<Float64Type>(columns.remove(0)), vec![Some(f32::MAX as f64)]);
        assert_eq!(parse_single_column::<Float64Type>(columns.remove(0)), vec![Some(f64::MAX)]);

        // If this failed it means that we forgot to assert some columns
        assert_eq!(columns.len(), 0, "There should be no more columns left in the results");
    }
}
