#[cfg(test)]
mod scalars_tests {
    /// These tests check that calling greatest only on scalars values
    ///
    /// This does not include columns
    ///

    use crate::tests::utils::{create_context, create_empty_data_frame, get_result_as_matrix};
    use datafusion::arrow::datatypes::{Float32Type, Int8Type};
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

        let df = df.select(vec![vec![greatest.call(vec![lit(ScalarValue::Null), lit(ScalarValue::Null), lit(ScalarValue::Null)])]].concat()).unwrap();

        let results = get_result_as_matrix::<Int8Type>(df).await.unwrap().transpose();

        assert_eq!(results, vec![[None]]);
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
}
