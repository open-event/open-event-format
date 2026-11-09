use super::constant::{EXTERNAL_ID_MAX_BYTES, SUBJECT_SEQUENCE_TARGET_LEN};
use super::provenance::hex_lower_into;
use crate::columns::column_ids;
use crate::error::RelationshipError;

pub const EVENT_ID_TARGET_LEN: usize = 16;

pub const PROTOCOL_TARGET_LEN: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum RelationshipKind {
    Auth,
    Link,
    Parent,
    Prev,
    Related,
    Root,
}

impl RelationshipKind {
    pub const ALL: [RelationshipKind; 6] = [
        RelationshipKind::Auth,
        RelationshipKind::Link,
        RelationshipKind::Parent,
        RelationshipKind::Prev,
        RelationshipKind::Related,
        RelationshipKind::Root,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            RelationshipKind::Auth => "auth",
            RelationshipKind::Link => "link",
            RelationshipKind::Parent => "parent",
            RelationshipKind::Prev => "prev",
            RelationshipKind::Related => "related",
            RelationshipKind::Root => "root",
        }
    }

    #[allow(
        clippy::should_implement_trait,
        reason = "an unknown tag is `None`, not a parse error"
    )]
    pub fn from_str(tag: &str) -> Option<Self> {
        match tag {
            "auth" => Some(RelationshipKind::Auth),
            "link" => Some(RelationshipKind::Link),
            "parent" => Some(RelationshipKind::Parent),
            "prev" => Some(RelationshipKind::Prev),
            "related" => Some(RelationshipKind::Related),
            "root" => Some(RelationshipKind::Root),
            _ => None,
        }
    }

    pub const fn column_id(self) -> u32 {
        match self {
            RelationshipKind::Auth => column_ids::AUTH_REFS,
            RelationshipKind::Link => column_ids::LINKED_REFS,
            RelationshipKind::Parent => column_ids::PARENT_REF,
            RelationshipKind::Prev => column_ids::PREV_REFS,
            RelationshipKind::Related => column_ids::RELATED_REFS,
            RelationshipKind::Root => column_ids::ROOT_REF,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TargetIdSpace {
    EventId,
    ExternalId,
    ProtocolEventId,
    SubjectSequence,
}

impl TargetIdSpace {
    pub const ALL: [TargetIdSpace; 4] = [
        TargetIdSpace::EventId,
        TargetIdSpace::ExternalId,
        TargetIdSpace::ProtocolEventId,
        TargetIdSpace::SubjectSequence,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            TargetIdSpace::EventId => "event_id",
            TargetIdSpace::ExternalId => "external_id",
            TargetIdSpace::ProtocolEventId => "protocol_event_id",
            TargetIdSpace::SubjectSequence => "subject_sequence",
        }
    }

    #[allow(
        clippy::should_implement_trait,
        reason = "an unknown tag is `None`, not a parse error"
    )]
    pub fn from_str(tag: &str) -> Option<Self> {
        match tag {
            "event_id" => Some(TargetIdSpace::EventId),
            "external_id" => Some(TargetIdSpace::ExternalId),
            "protocol_event_id" => Some(TargetIdSpace::ProtocolEventId),
            "subject_sequence" => Some(TargetIdSpace::SubjectSequence),
            _ => None,
        }
    }

    pub const fn fixed_len(self) -> Option<usize> {
        match self {
            TargetIdSpace::EventId => Some(EVENT_ID_TARGET_LEN),
            TargetIdSpace::ExternalId => None,
            TargetIdSpace::ProtocolEventId => Some(PROTOCOL_TARGET_LEN),
            TargetIdSpace::SubjectSequence => Some(SUBJECT_SEQUENCE_TARGET_LEN),
        }
    }

    pub const fn accepts_len(self, len: usize) -> bool {
        match self.fixed_len() {
            Some(fixed) => len == fixed,
            None => len >= 1 && len <= EXTERNAL_ID_MAX_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RelationshipRef {
    pub kind: RelationshipKind,
    pub space: TargetIdSpace,
    pub target_ref: Vec<u8>,
}

impl RelationshipRef {
    pub fn new(kind: RelationshipKind, space: TargetIdSpace, target_ref: Vec<u8>) -> Result<Self, RelationshipError> {
        if !space.accepts_len(target_ref.len()) {
            return Err(RelationshipError::WrongTargetLength);
        }
        Ok(Self {
            kind,
            space,
            target_ref,
        })
    }

    pub fn to_event(kind: RelationshipKind, event_id: u128) -> Self {
        Self {
            kind,
            space: TargetIdSpace::EventId,
            target_ref: event_id.to_be_bytes().to_vec(),
        }
    }

    pub fn to_external(kind: RelationshipKind, external_id: &[u8]) -> Result<Self, RelationshipError> {
        Self::new(kind, TargetIdSpace::ExternalId, external_id.to_vec())
    }

    pub fn to_protocol(kind: RelationshipKind, protocol_event_id: [u8; 32]) -> Self {
        Self {
            kind,
            space: TargetIdSpace::ProtocolEventId,
            target_ref: protocol_event_id.to_vec(),
        }
    }

    pub fn to_subject_sequence(kind: RelationshipKind, sn: u64) -> Self {
        Self {
            kind,
            space: TargetIdSpace::SubjectSequence,
            target_ref: sn.to_be_bytes().to_vec(),
        }
    }

    pub fn column_value(&self) -> String {
        let mut out = String::new();
        self.column_value_into(&mut out);
        out
    }

    pub fn column_value_into(&self, out: &mut String) {
        out.push_str(self.space.as_str());
        out.push(':');
        hex_lower_into(&self.target_ref, out);
    }

    pub fn parse_column_value(kind: RelationshipKind, value: &str) -> Result<Self, RelationshipError> {
        let (space_tag, hex) = value.split_once(':').ok_or(RelationshipError::MalformedColumnValue)?;
        let space = TargetIdSpace::from_str(space_tag).ok_or(RelationshipError::UnknownSpace)?;
        if !hex.len().is_multiple_of(2)
            || !space.accepts_len(hex.len() / 2)
            || hex.bytes().any(|byte| byte.is_ascii_uppercase())
        {
            return Err(RelationshipError::MalformedColumnValue);
        }
        let target_ref = hex_simd::decode_to_vec(hex).map_err(|_| RelationshipError::MalformedColumnValue)?;
        Self::new(kind, space, target_ref)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRelationships {
    refs: Vec<RelationshipRef>,
}

impl EventRelationships {
    pub fn new(refs: Vec<RelationshipRef>) -> Result<Self, RelationshipError> {
        let parents = refs.iter().filter(|r| r.kind == RelationshipKind::Parent).count();
        if parents > 1 {
            return Err(RelationshipError::MoreThanOneParent);
        }
        let roots = refs.iter().filter(|r| r.kind == RelationshipKind::Root).count();
        if roots > 1 {
            return Err(RelationshipError::MoreThanOneRoot);
        }
        Ok(Self { refs })
    }

    pub fn refs(&self) -> &[RelationshipRef] {
        &self.refs
    }

    pub fn parent(&self) -> Option<&RelationshipRef> {
        self.refs.iter().find(|r| r.kind == RelationshipKind::Parent)
    }

    pub fn root(&self) -> Option<&RelationshipRef> {
        self.refs.iter().find(|r| r.kind == RelationshipKind::Root)
    }

    pub fn column_text(&self, kind: RelationshipKind) -> Option<String> {
        let mut out = String::new();
        self.column_text_into(kind, &mut out).then_some(out)
    }

    pub fn column_text_into(&self, kind: RelationshipKind, out: &mut String) -> bool {
        out.clear();
        for reference in self.refs.iter().filter(|r| r.kind == kind) {
            if !out.is_empty() {
                out.push(' ');
            }
            reference.column_value_into(out);
        }
        !out.is_empty()
    }

    pub fn parse_column_text(kind: RelationshipKind, text: &str) -> Result<Vec<RelationshipRef>, RelationshipError> {
        text.split(' ')
            .filter(|value| !value.is_empty())
            .map(|value| RelationshipRef::parse_column_value(kind, value))
            .collect()
    }
}

#[cfg(test)]
#[path = "test/relationships.rs"]
mod tests;
