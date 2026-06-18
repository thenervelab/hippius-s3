//! Identifier newtypes: node and file identities.
//!
//! Both are non-empty, trimmed strings stored as `Box<str>` (owned but frozen — no
//! growth, so the capacity field a `String` carries is dropped). Every construction
//! path funnels through [`validated_identifier`], so the non-empty invariant cannot
//! be bypassed.

use crate::error::{Error, Result};
use core::str::FromStr;

/// The single validation site for all identifier newtypes.
///
/// Trims surrounding whitespace and rejects empty/all-whitespace input. Storing
/// the trimmed form means `" node-a "` and `"node-a"` are the same identity.
fn validated_identifier(raw: &str, kind: &'static str) -> Result<Box<str>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Error::EmptyIdentifier { kind });
    }
    Ok(Box::from(trimmed))
}

macro_rules! identifier_newtype {
    ($(#[$meta:meta])* $name:ident, $kind:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[cfg_attr(feature = "serde", derive(serde::Deserialize))]
        #[cfg_attr(feature = "serde", serde(try_from = "String"))]
        pub struct $name(Box<str>);

        impl $name {
            /// The identifier as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl core::fmt::Display for $name {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl TryFrom<String> for $name {
            type Error = Error;

            fn try_from(raw: String) -> Result<Self> {
                validated_identifier(&raw, $kind).map(Self)
            }
        }

        impl FromStr for $name {
            type Err = Error;

            fn from_str(s: &str) -> Result<Self> {
                validated_identifier(s, $kind).map(Self)
            }
        }
    };
}

identifier_newtype!(
    /// Identity of an ingest node (the Postgres primary key for node state).
    NodeId, "NodeId"
);
identifier_newtype!(
    /// Identity of an uploaded file (the `CephFS` destination folder name); the GC
    /// reclaim unit.
    FileId, "FileId"
);

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{FileId, NodeId};
    use crate::error::Error;
    use core::str::FromStr;
    use proptest::prelude::*;

    #[test]
    fn rejects_empty_and_whitespace() {
        assert!(matches!(NodeId::try_from(String::new()), Err(Error::EmptyIdentifier { kind: "NodeId" })));
        assert!(matches!(NodeId::from_str("   \t "), Err(Error::EmptyIdentifier { kind: "NodeId" })));
    }

    #[test]
    fn trims_surrounding_whitespace() {
        assert_eq!(NodeId::from_str("  node-a  ").unwrap().as_str(), "node-a");
    }

    #[test]
    fn each_identifier_kind_validates() {
        assert!(FileId::try_from(String::new()).is_err());
        assert!(matches!(FileId::from_str("   "), Err(Error::EmptyIdentifier { kind: "FileId" })));
        assert_eq!(FileId::from_str("file-1").unwrap().as_str(), "file-1");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_deserialization_routes_through_the_non_empty_check() {
        // The `#[serde(try_from = "String")]` attribute funnels wire data through the
        // same validation, so an empty string is a deserialize error, not a value.
        assert_eq!(serde_json::from_str::<NodeId>("\"node-a\"").unwrap().as_str(), "node-a");
        assert!(serde_json::from_str::<NodeId>("\"\"").is_err());
        assert!(serde_json::from_str::<NodeId>("\"   \"").is_err());
    }

    proptest! {
        /// Any string with non-whitespace content constructs and exposes its trim.
        #[test]
        fn non_empty_trims_and_round_trips(s in "[a-zA-Z0-9_./:-]{1,40}") {
            let id = NodeId::from_str(&s).unwrap();
            prop_assert_eq!(id.as_str(), s.trim());
        }

        /// Any all-whitespace string is rejected.
        #[test]
        fn all_whitespace_rejected(s in "[ \t\r\n]{0,8}") {
            prop_assert!(NodeId::from_str(&s).is_err());
        }
    }
}
