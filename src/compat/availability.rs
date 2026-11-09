use crate::error::FormatError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AvailabilityWindow {
    pub introduced: (u16, u16),
    pub retired: Option<(u16, u16)>,
    pub transform_id: u32,
}

pub const PINNED_AVAILABILITY_WINDOWS: [AvailabilityWindow; 17] = [
    AvailabilityWindow {
        introduced: (1, 0),
        retired: None,
        transform_id: 0,
    },
    AvailabilityWindow {
        introduced: (1, 0),
        retired: None,
        transform_id: 1,
    },
    AvailabilityWindow {
        introduced: (1, 0),
        retired: None,
        transform_id: 2,
    },
    AvailabilityWindow {
        introduced: (1, 0),
        retired: None,
        transform_id: 3,
    },
    AvailabilityWindow {
        introduced: (1, 0),
        retired: None,
        transform_id: 4,
    },
    AvailabilityWindow {
        introduced: (1, 0),
        retired: None,
        transform_id: 5,
    },
    AvailabilityWindow {
        introduced: (1, 0),
        retired: None,
        transform_id: 6,
    },
    AvailabilityWindow {
        introduced: (1, 0),
        retired: None,
        transform_id: 7,
    },
    AvailabilityWindow {
        introduced: (1, 0),
        retired: None,
        transform_id: 8,
    },
    AvailabilityWindow {
        introduced: (1, 0),
        retired: None,
        transform_id: 9,
    },
    AvailabilityWindow {
        introduced: (1, 0),
        retired: None,
        transform_id: 10,
    },
    AvailabilityWindow {
        introduced: (1, 0),
        retired: Some((1, 0)),
        transform_id: 11,
    },
    AvailabilityWindow {
        introduced: (1, 0),
        retired: None,
        transform_id: 12,
    },
    AvailabilityWindow {
        introduced: (1, 0),
        retired: None,
        transform_id: 13,
    },
    AvailabilityWindow {
        introduced: (1, 0),
        retired: None,
        transform_id: 14,
    },
    AvailabilityWindow {
        introduced: (1, 0),
        retired: None,
        transform_id: 15,
    },
    AvailabilityWindow {
        introduced: (1, 0),
        retired: None,
        transform_id: 16,
    },
];

pub fn validate_pipeline_window(
    pipeline_id: u32,
    format_version: (u16, u16),
    windows: &[AvailabilityWindow],
) -> Result<(), FormatError> {
    let transform_id = pipeline_id & 0xFF;
    let Some(window) = windows.iter().find(|window| window.transform_id == transform_id) else {
        return Err(FormatError::Structural {
            rule: "pipeline transform has no availability window",
        });
    };
    if format_version < window.introduced {
        return Err(FormatError::Structural {
            rule: "pipeline introduced after the file's declared format version",
        });
    }
    if window.retired.is_some_and(|retired| format_version >= retired) {
        return Err(FormatError::Structural {
            rule: "pipeline retired at or before the file's declared format version",
        });
    }
    Ok(())
}

pub fn validate_pipeline_window_pinned(pipeline_id: u32, format_version: (u16, u16)) -> Result<(), FormatError> {
    validate_pipeline_window(pipeline_id, format_version, &PINNED_AVAILABILITY_WINDOWS)
}

#[cfg(test)]
#[path = "test/availability.rs"]
mod tests;
