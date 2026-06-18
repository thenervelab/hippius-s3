//! The hippius-s3 api's part identity and its on-disk path mapping.
//!
//! The api lays an object's chunks out as `<object_id>/v<version>/part_<n>/`
//! containing `chunk_<index>.bin` files and a `meta.json` part-complete marker
//! (see `hippius_s3/cache/fs_store.py`). The drain replicates that tree
//! **path-preservingly** from node-local SSD to `CephFS`, so its unit is the
//! **part** — `(object_id, version, part_number)` — not a content-addressed chunk.
//!
//! This module is the pure identity + path-mapping core: the newtypes that name a
//! part, the traversal-safe render of a [`PartKey`] to a relative directory, and the
//! inverse parse a reconciler scan uses. It performs no I/O.

use crate::error::Error;
use core::str::FromStr;
use std::path::{Component, Path, PathBuf};
use thiserror::Error as ThisError;

/// A hippius-s3 object id: the api names each object's cache folder by its UUID.
///
/// Stored as the validated UUID string (`Box<str>`, frozen). Construction funnels
/// through [`validated_object_id`], so a held value is always a hyphenated UUID —
/// which is inherently a single safe path component (no separators, `..`, or NUL),
/// closing the traversal hole a raw folder name would open.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String"))]
pub struct ObjectId(Box<str>);

impl ObjectId {
    /// The object id as a string slice (the canonical hyphenated UUID).
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The single validation site for [`ObjectId`]: a canonical 8-4-4-4-12 hyphenated
/// UUID. Rejecting anything else is both correctness (the api uses UUID folders)
/// and the traversal guard.
fn validated_object_id(raw: &str) -> Result<Box<str>, Error> {
    if is_uuid_shaped(raw) {
        Ok(Box::from(raw))
    } else {
        Err(Error::InvalidObjectId { value: raw.to_owned() })
    }
}

/// Whether `s` is a canonical 8-4-4-4-12 hyphenated UUID (either hex case). ASCII
/// by construction, so it can hold no path separator, `..`, or NUL.
fn is_uuid_shaped(s: &str) -> bool {
    if s.len() != 36 {
        return false;
    }
    s.bytes().enumerate().all(|(i, byte)| match i {
        8 | 13 | 18 | 23 => byte == b'-',
        _ => byte.is_ascii_hexdigit(),
    })
}

impl TryFrom<String> for ObjectId {
    type Error = Error;

    fn try_from(raw: String) -> Result<Self, Error> {
        validated_object_id(&raw).map(Self)
    }
}

impl FromStr for ObjectId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        validated_object_id(s).map(Self)
    }
}

impl core::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.0)
    }
}

macro_rules! numeric_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u32);

        impl $name {
            /// Wraps a raw value.
            #[must_use]
            pub const fn new(value: u32) -> Self {
                Self(value)
            }

            /// The raw value.
            #[must_use]
            pub const fn get(self) -> u32 {
                self.0
            }
        }

        impl core::fmt::Display for $name {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

numeric_newtype!(
    /// An object's version number (the `v<version>` path segment).
    Version
);
numeric_newtype!(
    /// A part number within a version (the `part_<n>` path segment; one-based).
    PartNumber
);
numeric_newtype!(
    /// A chunk index within a part (the `chunk_<index>.bin` file; zero-based).
    ChunkIndex
);

/// The part-complete marker file the api writes last; the drain copies it last too,
/// so a reader's `meta.json` gate flips only once the whole part is on `CephFS`.
pub const META_FILE_NAME: &str = "meta.json";

/// The drain unit: one part of one object version.
///
/// Names the `<object_id>/v<version>/part_<n>/` directory the api writes and the
/// drain replicates path-preservingly to `CephFS`. The fields are private so a
/// `PartKey` can only be built from an already-validated [`ObjectId`] and typed
/// version/part numbers — there is no raw-string constructor to bypass.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartKey {
    object: ObjectId,
    version: Version,
    part: PartNumber,
}

impl PartKey {
    /// Names the part `(object, version, part)`.
    #[must_use]
    pub fn new(object: ObjectId, version: Version, part: PartNumber) -> Self {
        Self { object, version, part }
    }

    /// The owning object id.
    #[must_use]
    pub fn object(&self) -> &ObjectId {
        &self.object
    }

    /// The object version.
    #[must_use]
    pub fn version(&self) -> Version {
        self.version
    }

    /// The part number.
    #[must_use]
    pub fn part(&self) -> PartNumber {
        self.part
    }

    /// The traversal-safe relative directory `<object>/v<version>/part_<n>`.
    ///
    /// Built by pushing each segment onto a [`PathBuf`] rather than concatenating
    /// strings (axiom 164): the object id is a validated UUID and the version/part
    /// segments are formatted from `u32`, so no segment can carry a separator or
    /// `..`, and the result stays confined to a single part folder.
    #[must_use]
    pub fn relative_dir(&self) -> PathBuf {
        let mut path = PathBuf::from(self.object.as_str());
        path.push(format!("v{}", self.version.get()));
        path.push(format!("part_{}", self.part.get()));
        path
    }
}

/// The chunk file name `chunk_<index>.bin` the api writes within a part dir.
#[must_use]
pub fn chunk_file_name(index: ChunkIndex) -> String {
    format!("chunk_{}.bin", index.get())
}

/// A failure parsing a scanned relative directory into a [`PartKey`].
///
/// `#[non_exhaustive]`: the reconciler treats every malformed entry the same way
/// (skip it), so growing the taxonomy never breaks its match.
#[derive(Debug, ThisError)]
#[non_exhaustive]
pub enum PartPathError {
    /// A component was not a plain name — an absolute root, `..`, `.`, or a Windows
    /// prefix. A scanned part dir is exactly three plain components; anything else
    /// is junk or a traversal attempt.
    #[error("path is not a confined relative directory")]
    Traversal,
    /// The path did not have exactly three components (`object/version/part`).
    #[error("expected 3 path components (object/version/part), found {found}")]
    ComponentCount {
        /// How many plain components were present.
        found: usize,
    },
    /// A component was not valid UTF-8 (paths are `OsStr`, not guaranteed UTF-8).
    #[error("path component is not valid UTF-8")]
    NonUtf8,
    /// The first component was not a UUID object id.
    #[error("object component {value:?} is not a UUID")]
    BadObject {
        /// The offending component.
        value: String,
    },
    /// The second component was not a `v<number>` version segment.
    #[error("version component {segment:?} is not `v<number>`")]
    BadVersion {
        /// The offending component.
        segment: String,
    },
    /// The third component was not a `part_<number>` segment.
    #[error("part component {segment:?} is not `part_<number>`")]
    BadPart {
        /// The offending component.
        segment: String,
    },
}

/// Parses a scanned relative `<object>/v<version>/part_<n>` directory into a
/// [`PartKey`] — the inverse of [`PartKey::relative_dir`].
///
/// Walks [`Path::components`] (never splitting on a separator, per axiom 164) and
/// admits exactly three plain components; any root/`..`/`.`/prefix component is a
/// [`PartPathError::Traversal`].
///
/// # Errors
///
/// A [`PartPathError`] naming which component failed: a traversal/non-plain
/// component, the wrong component count, a non-UTF-8 component, or a malformed
/// object/version/part segment.
pub fn parse_part_dir(rel: &Path) -> Result<PartKey, PartPathError> {
    let mut names: Vec<&str> = Vec::new();
    for component in rel.components() {
        // Only plain names are admitted; a root, `..`, `.`, or Windows prefix is a
        // traversal/absolute shape a confined part dir never has.
        let Component::Normal(raw) = component else {
            return Err(PartPathError::Traversal);
        };
        names.push(raw.to_str().ok_or(PartPathError::NonUtf8)?);
    }

    let [object, version_seg, part_seg] = names.as_slice() else {
        return Err(PartPathError::ComponentCount { found: names.len() });
    };

    let object = ObjectId::from_str(object).map_err(|_| PartPathError::BadObject { value: (*object).to_owned() })?;
    let version = parse_segment(version_seg, "v").ok_or_else(|| PartPathError::BadVersion {
        segment: (*version_seg).to_owned(),
    })?;
    let part = parse_segment(part_seg, "part_").ok_or_else(|| PartPathError::BadPart {
        segment: (*part_seg).to_owned(),
    })?;
    Ok(PartKey::new(object, Version::new(version), PartNumber::new(part)))
}

/// Strips `prefix` from `segment` and parses the rest as a `u32`, or `None` if the
/// prefix is absent or the remainder is not a bare non-negative integer.
fn parse_segment(segment: &str, prefix: &str) -> Option<u32> {
    segment.strip_prefix(prefix)?.parse::<u32>().ok()
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{ChunkIndex, ObjectId, PartKey, PartNumber, PartPathError, Version, chunk_file_name, parse_part_dir};
    use crate::error::Error;
    use core::str::FromStr;
    use proptest::prelude::*;
    use std::path::{Path, PathBuf};

    const UUID: &str = "466916c0-d61b-4518-b81b-9576b574270a";

    fn part_key(version: u32, part: u32) -> PartKey {
        PartKey::new(ObjectId::from_str(UUID).unwrap(), Version::new(version), PartNumber::new(part))
    }

    #[test]
    fn accepts_a_canonical_uuid() {
        assert_eq!(ObjectId::from_str(UUID).unwrap().as_str(), UUID);
    }

    #[test]
    fn rejects_a_non_uuid() {
        // A bare name, a traversal attempt, and a separator must all be rejected —
        // the api only ever creates UUID folders, so anything else is junk or attack.
        for bad in ["not-a-uuid", "..", "a/b", "", "466916c0d61b4518b81b9576b574270a"] {
            assert!(
                matches!(ObjectId::from_str(bad), Err(Error::InvalidObjectId { ref value }) if value == bad),
                "{bad:?} must be rejected as a non-UUID",
            );
        }
    }

    #[test]
    fn relative_dir_renders_the_object_version_part_layout() {
        let expected: PathBuf = [UUID, "v5", "part_1"].iter().collect();
        assert_eq!(part_key(5, 1).relative_dir(), expected);
    }

    #[test]
    fn chunk_file_name_is_indexed_with_a_bin_extension() {
        assert_eq!(chunk_file_name(ChunkIndex::new(0)), "chunk_0.bin");
        assert_eq!(chunk_file_name(ChunkIndex::new(42)), "chunk_42.bin");
    }

    #[test]
    fn parse_accepts_a_well_formed_part_dir() {
        let parsed = parse_part_dir(Path::new("466916c0-d61b-4518-b81b-9576b574270a/v5/part_1")).unwrap();
        assert_eq!(parsed, part_key(5, 1));
    }

    #[test]
    fn parse_rejects_the_wrong_component_count() {
        let two = parse_part_dir(Path::new("466916c0-d61b-4518-b81b-9576b574270a/v5")).unwrap_err();
        assert!(matches!(two, PartPathError::ComponentCount { found: 2 }));
        let four = parse_part_dir(Path::new("466916c0-d61b-4518-b81b-9576b574270a/v5/part_1/chunk_0.bin")).unwrap_err();
        assert!(matches!(four, PartPathError::ComponentCount { found: 4 }));
    }

    #[test]
    fn parse_rejects_a_traversal_or_absolute_component() {
        for bad in [
            "/466916c0-d61b-4518-b81b-9576b574270a/v5/part_1",
            "../466916c0-d61b-4518-b81b-9576b574270a/v5/part_1",
        ] {
            assert!(matches!(parse_part_dir(Path::new(bad)), Err(PartPathError::Traversal)), "{bad:?}");
        }
    }

    #[test]
    fn parse_rejects_a_non_uuid_object_component() {
        let err = parse_part_dir(Path::new("not-a-uuid/v5/part_1")).unwrap_err();
        assert!(matches!(err, PartPathError::BadObject { ref value } if value == "not-a-uuid"));
    }

    #[test]
    fn parse_rejects_malformed_version_and_part_segments() {
        let bad_v = parse_part_dir(Path::new("466916c0-d61b-4518-b81b-9576b574270a/version5/part_1")).unwrap_err();
        assert!(matches!(bad_v, PartPathError::BadVersion { ref segment } if segment == "version5"));
        let bad_p = parse_part_dir(Path::new("466916c0-d61b-4518-b81b-9576b574270a/v5/p1")).unwrap_err();
        assert!(matches!(bad_p, PartPathError::BadPart { ref segment } if segment == "p1"));
    }

    proptest! {
        /// Render then parse round-trips: a `PartKey` survives a trip through its
        /// on-disk relative path unchanged — the contract the reconciler relies on.
        #[test]
        fn render_then_parse_round_trips(
            object in "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            version in 0u32..=u32::MAX,
            part in 0u32..=u32::MAX,
        ) {
            let key = PartKey::new(ObjectId::from_str(&object).unwrap(), Version::new(version), PartNumber::new(part));
            prop_assert_eq!(parse_part_dir(&key.relative_dir()).unwrap(), key);
        }

        /// The parser is a boundary over scanned directory names: arbitrary text must
        /// yield a typed error or a key, never a panic.
        #[test]
        fn parse_never_panics_on_arbitrary_input(raw in ".*") {
            let _ = parse_part_dir(Path::new(&raw));
        }
    }
}
