use datafusion::arrow::buffer::NullBuffer;

pub(crate) trait NullBufferExt {

    /// Same as `NullBuffer::union` but if one of the buffers is `None` we return None
    ///
    /// This is useful when we want to prefer not null values
    fn union_prefer_not_null(lhs: Option<&Self>, rhs: Option<&Self>) -> Option<Self> where Self: Sized;

    /// Given two null buffers, return a new null buffer with nulls when both buffers have nulls at the same index
    ///
    /// Example:
    /// If we have two null buffers:
    /// 1: [true, true, false, false]
    /// 2: [true, false, true, false]
    ///
    /// The result will be:
    /// [true, false, false, false] (only when both buffers have nulls at the same index the value will be null)
    ///
    /// If one of the buffers is None, the other buffer will be returned
    /// If both buffers are None, None will be returned
    ///
    fn intersect(lhs: Option<&Self>, rhs: Option<&Self>) -> Option<Self> where Self: Sized;

    // /// Given two null buffers, return a new null buffer with nulls when both buffers have nulls at the same index
    // ///
    // /// Example:
    // /// If we have two null buffers:
    // /// 1: [true, true, false, false]
    // /// 2: [true, false, true, false]
    // ///
    // /// The result will be:
    // /// [true, false, false, false] (only when both buffers have nulls at the same index the value will be null)
    // ///
    // /// If one of the buffers is None, the other buffer will be returned
    // /// If both buffers are None, None will be returned
    // ///
    // fn symmetric_difference(lhs: Option<&Self>, rhs: Option<&Self>) -> Option<Self> where Self: Sized;
}

impl NullBufferExt for NullBuffer {
    fn union_prefer_not_null(lhs: Option<&Self>, rhs: Option<&Self>) -> Option<Self>
    where
        Self: Sized
    {
        match (lhs, rhs) {
            (Some(lhs), Some(rhs)) => Some(Self::new(lhs.inner() | rhs.inner())),
            (_, None) | (None, _) => None,
        }
    }

    fn intersect(lhs: Option<&Self>, rhs: Option<&Self>) -> Option<Self> {
        match (lhs, rhs) {
            (Some(lhs), Some(rhs)) => Some(Self::new(lhs.inner() & rhs.inner())),
            (Some(n), None) | (None, Some(n)) => Some(n.clone()),
            (None, None) => None,
        }
    }
}


#[cfg(test)]
mod tests {
    use datafusion::arrow::buffer::NullBuffer;
    use super::NullBufferExt;


    #[test]
    fn union_prefer_not_null() {
        assert_eq!(NullBuffer::union_prefer_not_null(None, None), None);
        assert_eq!(NullBuffer::union_prefer_not_null(Some(&NullBuffer::new_null(2)), None), None);
        assert_eq!(NullBuffer::union_prefer_not_null(Some(&NullBuffer::new_valid(2)), None), None);
        assert_eq!(NullBuffer::union_prefer_not_null(None, Some(&NullBuffer::new_null(2))), None);
        assert_eq!(NullBuffer::union_prefer_not_null(None, Some(&NullBuffer::new_valid(2))), None);

        assert_eq!(NullBuffer::union_prefer_not_null(Some(&NullBuffer::new_null(2)), Some(&NullBuffer::new_null(2))), Some(NullBuffer::new_null(2)));
        assert_eq!(NullBuffer::union_prefer_not_null(Some(&NullBuffer::new_valid(2)), Some(&NullBuffer::new_null(2))), Some(NullBuffer::new_valid(2)));
        assert_eq!(NullBuffer::union_prefer_not_null(Some(&NullBuffer::new_null(2)), Some(&NullBuffer::new_valid(2))), Some(NullBuffer::new_valid(2)));
        assert_eq!(NullBuffer::union_prefer_not_null(Some(&NullBuffer::new_valid(2)), Some(&NullBuffer::new_valid(2))), Some(NullBuffer::new_valid(2)));
        assert_eq!(
            NullBuffer::union_prefer_not_null(
                Some(&NullBuffer::from([true, false, true, false].as_slice())),
                Some(&NullBuffer::from([true, true, false, false].as_slice()))
            ),
            Some(NullBuffer::from([true, true, true, false].as_slice()))
        );
    }

    #[test]
    fn intersection_both_none() {
        let result = NullBuffer::intersect(None, None);
        assert_eq!(result, None);
    }

    #[test]
    fn intersection_one_none() {
        let null_buffer = NullBuffer::from([true, true, true, true].as_slice());
        let result = NullBuffer::intersect(
            Some(&null_buffer),
            None
        );
        assert_eq!(result, Some(null_buffer));


        let null_buffer = NullBuffer::new_null(2);
        let result = NullBuffer::intersect(
            None,
            Some(&null_buffer),
        );
        assert_eq!(result, Some(null_buffer));


        let null_valid_buffer = NullBuffer::new_valid(2);
        let result = NullBuffer::intersect(
            Some(&null_valid_buffer),
            None
        );
        assert_eq!(result, Some(null_valid_buffer));


        let null_valid_buffer = NullBuffer::new_valid(2);
        let result = NullBuffer::intersect(
            None,
            Some(&null_valid_buffer),
        );
        assert_eq!(result, Some(null_valid_buffer));
    }

    #[test]
    fn intersection_both_some() {
        let null_buffer1 = NullBuffer::new_null(2);
        let null_buffer2 = NullBuffer::new_null(2);
        let result = NullBuffer::intersect(
            Some(&null_buffer1),
            Some(&null_buffer2),
        );

        // everything is null
        assert_eq!(result, Some(null_buffer1.clone()));


        let null_valid_buffer1 = NullBuffer::new_valid(2);
        let null_buffer2 = NullBuffer::new_null(2);
        let result = NullBuffer::intersect(
            Some(&null_valid_buffer1),
            Some(&null_buffer2),
        );

        // take nulls as they are different
        assert_eq!(result, Some(null_buffer2.clone()));


        let null_buffer1 = NullBuffer::new_null(2);
        let null_valid_buffer2 = NullBuffer::new_valid(2);
        let result = NullBuffer::intersect(
            Some(&null_buffer1),
            Some(&null_valid_buffer2),
        );

        // take nulls as they are different
        assert_eq!(result, Some(null_buffer1.clone()));

        let null_valid_buffer1 = NullBuffer::new_valid(2);
        let null_valid_buffer2 = NullBuffer::new_valid(2);
        let result = NullBuffer::intersect(
            Some(&null_valid_buffer1),
            Some(&null_valid_buffer2),
        );

        // both valid
        assert_eq!(result, Some(NullBuffer::from([false, false].as_slice())));
    }

    #[test]
    fn intersection_both_some_with_interleaving() {
        let null_buffer1 = NullBuffer::from([true, true, true, true].as_slice());
        let null_buffer2 = NullBuffer::from([true, true, true, true].as_slice());
        let result = NullBuffer::intersect(
            Some(&null_buffer1),
            Some(&null_buffer2),
        );

        // they are the same, keep as is
        assert_eq!(result, Some(null_buffer1));

        let null_buffer1 = NullBuffer::from([false, false, false, false].as_slice());
        let null_buffer2 = NullBuffer::from([false, false, false, false].as_slice());
        let result = NullBuffer::intersect(
            Some(&null_buffer1),
            Some(&null_buffer2),
        );

        // they are the same, keep as is
        assert_eq!(result, Some(null_buffer1));

        let null_buffer1 = NullBuffer::from([true, false, false, true].as_slice());
        let null_buffer2 = NullBuffer::from([true, false, false, true].as_slice());
        let result = NullBuffer::intersect(
            Some(&null_buffer1),
            Some(&null_buffer2),
        );

        // they are the same, keep as is
        assert_eq!(result, Some(null_buffer1));

        let null_buffer1 = NullBuffer::from([false, true, true, false].as_slice());
        let null_buffer2 = NullBuffer::from([true, false, false, true].as_slice());
        let result = NullBuffer::intersect(
            Some(&null_buffer1),
            Some(&null_buffer2),
        );

        // they have nothing in common
        assert_eq!(result, Some(NullBuffer::from([false, false, false, false].as_slice())));

        let null_buffer1 = NullBuffer::from([true, true, false, false].as_slice());
        let null_buffer2 = NullBuffer::from([true, false, true, false].as_slice());
        let result = NullBuffer::intersect(
            Some(&null_buffer1),
            Some(&null_buffer2),
        );

        // Keep only common
        assert_eq!(result, Some(NullBuffer::from([true, false, false, false].as_slice())));
    }
}
