use datafusion::arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::prelude::DataFrame;

/// This will return the result of a DataFrame as a matrix.
/// Each row in the matrix corresponds to a column in the DataFrame.
/// Each column in the matrix corresponds to a row in the DataFrame.
///
/// If you want to have the rows in the matrix to correspond to the rows in the DataFrame,
/// you can use the `transpose` method on the resulting matrix.
pub(crate) async fn get_primitive_result_as_matrix<PrimitiveType: ArrowPrimitiveType>(df: DataFrame) -> Result<Vec<Vec<Option<PrimitiveType::Native>>>> {
    let columns = get_combined_results(df).await?;

    Ok(
        columns
            .columns()
            .iter()
            .map(|column| parse_primitive_column::<PrimitiveType>(column))
            .collect::<Vec<_>>()
    )
}

/// This will return the result of a DataFrame as a matrix.
/// Each row in the matrix corresponds to a column in the DataFrame.
/// Each column in the matrix corresponds to a row in the DataFrame.
///
/// If you want to have the rows in the matrix to correspond to the rows in the DataFrame,
/// you can use the `transpose` method on the resulting matrix.
pub(crate) async fn get_string_result_as_matrix(df: DataFrame) -> Result<Vec<Vec<Option<String>>>> {
    let columns = get_combined_results(df).await?;

    Ok(
        columns
            .columns()
            .iter()
            .map(|column| parse_string_column(column))
            .collect::<Vec<_>>()
    )
}

/// This will return the result of a DataFrame as a matrix.
/// Each row in the matrix corresponds to a column in the DataFrame.
/// Each column in the matrix corresponds to a row in the DataFrame.
///
/// If you want to have the rows in the matrix to correspond to the rows in the DataFrame,
/// you can use the `transpose` method on the resulting matrix.
pub(crate) async fn get_list_result_as_matrix<T: ArrowPrimitiveType>(df: DataFrame) -> Result<Vec<Vec<Option<Vec<Option<T::Native>>>>>> {
    let columns = get_combined_results(df).await?;

    Ok(
        columns
            .columns()
            .iter()
            .map(|column| parse_list_column::<T>(column))
            .collect::<Vec<_>>()
    )
}

pub(crate) async fn get_combined_results<'a>(df: DataFrame) -> Result<RecordBatch> {
    let schema = df.schema().clone();

    concat_batches(schema.as_ref(), df.collect().await?.iter()).map_err(|e| e.into())
}

/// Parse many columns with the same type
///
/// # Safety
/// This will panic if the column is not of the expected type.
pub(crate) fn parse_many_primitives_columns<PrimitiveType: ArrowPrimitiveType>(columns: &[&ArrayRef]) -> Vec<Vec<Option<PrimitiveType::Native>>> {
    columns
        .iter()
        .map(|column| parse_primitive_column::<PrimitiveType>(column))
        .collect::<Vec<_>>()
}

/// Parse single column
///
/// # Safety
/// This will panic if the column is not of the expected type.
pub(crate) fn parse_primitive_column<PrimitiveType: ArrowPrimitiveType>(column: &ArrayRef) -> Vec<Option<PrimitiveType::Native>> {
    assert_eq!(column.data_type(), &PrimitiveType::DATA_TYPE);

    if column.data_type().is_null() {
        return vec![None; column.len()];
    }

    column
        .as_primitive_opt::<PrimitiveType>()
        // TODO - print the found results on failure
        .expect("Unable to downcast to expected PrimitiveArray")
        .values()
        .iter()
        .enumerate()
        .map(|(index, value)| if column.is_null(index) { None } else { Some(*value) })
        .collect::<Vec<_>>()
}

/// Parse single column
pub(crate) fn parse_string_column(column: &ArrayRef) -> Vec<Option<String>> {
    if column.data_type().is_null() {
        return vec![None; column.len()];
    }

    let values = column.as_string_opt::<i32>()
        .expect("Unable to downcast to expected StringArray");

    (0..values.len())
        .map(|index| {
            if column.is_null(index) {
                None
            } else {
                Some(values.value(index).to_string())
            }
        })
        .collect::<Vec<_>>()

}

/// Parse single column
pub(crate) fn parse_list_column<T: ArrowPrimitiveType>(column: &ArrayRef) -> Vec<Option<Vec<Option<T::Native>>>> {
    if column.data_type().is_null() {
        return vec![None; column.len()];
    }

    let values = column.as_list_opt::<i32>()
        .expect("Unable to downcast to expected StringArray");

    (0..values.len())
        .map(|index| {
            if column.is_null(index) {
                None
            } else {
                Some(parse_primitive_column::<T>(&values.value(index)))
            }
        })
        .collect::<Vec<_>>()

}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, Int8Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Int8Type, Schema};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_get_result_as_matrix_i32() {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));


        let a_vec: Vec<i32> = vec![1, 10, 10, 100];
        let b_vec: Vec<i32> = vec![4, 20, 30, 500];
        let c_vec: Vec<i32> = vec![5, 70, 60, 700];


        // define data.
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(a_vec.clone())),
                Arc::new(Int32Array::from(b_vec.clone())),
                Arc::new(Int32Array::from(c_vec.clone())),
            ],
        ).unwrap();

        // declare a new context. In spark API, this corresponds to a new spark SQLsession
        let ctx = SessionContext::new();

        // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
        ctx.register_batch("t", batch).unwrap();
        let df = ctx.table("t").await.unwrap();

        let df = df.select_columns(&["a", "b", "c"]).unwrap();
        let results = get_primitive_result_as_matrix::<Int32Type>(df).await.unwrap();

        assert_eq!(results, vec![
            a_vec.iter().map(|item: &i32| Some(*item)).collect::<Vec<_>>(),
            b_vec.iter().map(|item: &i32| Some(*item)).collect::<Vec<_>>(),
            c_vec.iter().map(|item: &i32| Some(*item)).collect::<Vec<_>>()
        ])
    }

    #[tokio::test]
    async fn test_get_result_as_matrix_i8() {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("b", DataType::Int8, false),
            Field::new("c", DataType::Int8, false),
        ]));


        let a_vec: Vec<i8> = vec![1, 10, 10, 100];
        let b_vec: Vec<i8> = vec![4, 20, 30, 40];
        let c_vec: Vec<i8> = vec![5, 70, 60, 80];


        // define data.
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int8Array::from(a_vec.clone())),
                Arc::new(Int8Array::from(b_vec.clone())),
                Arc::new(Int8Array::from(c_vec.clone())),
            ],
        ).unwrap();

        // declare a new context. In spark API, this corresponds to a new spark SQLsession
        let ctx = SessionContext::new();

        // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
        ctx.register_batch("t", batch).unwrap();
        let df = ctx.table("t").await.unwrap();

        let df = df.select_columns(&["a", "b", "c"]).unwrap();
        let results = get_primitive_result_as_matrix::<Int8Type>(df).await.unwrap();

        assert_eq!(results, vec![
            a_vec.iter().map(|item: &i8| Some(*item)).collect::<Vec<_>>(),
            b_vec.iter().map(|item: &i8| Some(*item)).collect::<Vec<_>>(),
            c_vec.iter().map(|item: &i8| Some(*item)).collect::<Vec<_>>()
        ])
    }

    #[tokio::test]
    async fn test_get_result_as_matrix_i8_with_null() {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, true),
            Field::new("b", DataType::Int8, false),
            Field::new("c", DataType::Int8, false),
        ]));


        let a_vec: Vec<Option<i8>> = vec![Some(1), None, None, Some(100)];
        let b_vec: Vec<Option<i8>> = vec![4, 20, 30, 40].iter().map(|item: &i8| Some(*item)).collect::<Vec<_>>();
        let c_vec: Vec<Option<i8>> = vec![5, 70, 60, 80].iter().map(|item: &i8| Some(*item)).collect::<Vec<_>>();


        // define data.
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int8Array::from(a_vec.clone())),
                Arc::new(Int8Array::from(b_vec.clone())),
                Arc::new(Int8Array::from(c_vec.clone())),
            ],
        ).unwrap();

        // declare a new context. In spark API, this corresponds to a new spark SQLsession
        let ctx = SessionContext::new();

        // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
        ctx.register_batch("t", batch).unwrap();
        let df = ctx.table("t").await.unwrap();

        let df = df.select_columns(&["a", "b", "c"]).unwrap();
        let results = get_primitive_result_as_matrix::<Int8Type>(df).await.unwrap();

        assert_eq!(results, vec![
            a_vec,
            b_vec,
            c_vec
        ])
    }

    #[tokio::test]
    async fn test_get_result_as_matrix_string_with_null() {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, false),
        ]));


        let a_vec: Vec<Option<String>> = vec![Some("hello".to_string()), None, Some("world".to_string()), None];
        let b_vec: Vec<Option<String>> = vec![Some("How".to_string()), Some("are".to_string()), Some("you".to_string()), Some("doing".to_string())];

        // define data.
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(a_vec.clone())),
                Arc::new(StringArray::from(b_vec.clone())),
            ],
        ).unwrap();

        // declare a new context. In spark API, this corresponds to a new spark SQLsession
        let ctx = SessionContext::new();

        // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
        ctx.register_batch("t", batch).unwrap();
        let df = ctx.table("t").await.unwrap();

        let df = df.select_columns(&["a", "b"]).unwrap();
        let results = get_string_result_as_matrix(df).await.unwrap();

        assert_eq!(results, vec![
            a_vec,
            b_vec,
        ])
    }
}

