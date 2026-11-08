//! Generates the same events for every format: an event envelope plus a typed payload, from a fixed seed.

pub const FIRST_OCCURRED_AT: i64 = 1_700_000_000_000_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    Chat,
    Messages,
    Telemetry,
}

impl Profile {
    pub const ALL: [Profile; 3] = [Profile::Telemetry, Profile::Messages, Profile::Chat];

    pub fn name(self) -> &'static str {
        match self {
            Profile::Chat => "chat",
            Profile::Messages => "messages",
            Profile::Telemetry => "telemetry",
        }
    }
}

#[derive(Clone, Debug)]
pub enum Column {
    F64(Vec<f64>),
    I64(Vec<i64>),
    Str(Vec<String>),
}

#[derive(Clone, Debug)]
pub struct Dataset {
    pub columns: Vec<(String, Column)>,
    pub profile: Profile,
    pub rows: usize,
}

impl Dataset {
    pub fn column(&self, name: &str) -> &Column {
        &self
            .columns
            .iter()
            .find(|(column, _)| column == name)
            .unwrap_or_else(|| panic!("no column {name}"))
            .1
    }

    pub fn i64s(&self, name: &str) -> &[i64] {
        match self.column(name) {
            Column::I64(values) => values,
            _ => panic!("{name} is not an integer column"),
        }
    }

    pub fn slice(&self, start: usize, end: usize) -> Dataset {
        Dataset {
            columns: self
                .columns
                .iter()
                .map(|(name, column)| {
                    let column = match column {
                        Column::F64(v) => Column::F64(v[start..end].to_vec()),
                        Column::I64(v) => Column::I64(v[start..end].to_vec()),
                        Column::Str(v) => Column::Str(v[start..end].to_vec()),
                    };
                    (name.clone(), column)
                })
                .collect(),
            profile: self.profile,
            rows: end - start,
        }
    }

    pub fn rows_without(&self, removed: &[usize]) -> Dataset {
        let drop: std::collections::HashSet<usize> = removed.iter().copied().collect();
        let keep = |i: &usize| !drop.contains(i);
        Dataset {
            columns: self
                .columns
                .iter()
                .map(|(name, column)| {
                    let column = match column {
                        Column::F64(v) => Column::F64((0..v.len()).filter(keep).map(|i| v[i]).collect()),
                        Column::I64(v) => Column::I64((0..v.len()).filter(keep).map(|i| v[i]).collect()),
                        Column::Str(v) => Column::Str((0..v.len()).filter(keep).map(|i| v[i].clone()).collect()),
                    };
                    (name.clone(), column)
                })
                .collect(),
            profile: self.profile,
            rows: self.rows - drop.len(),
        }
    }

    pub fn strs(&self, name: &str) -> &[String] {
        match self.column(name) {
            Column::Str(values) => values,
            _ => panic!("{name} is not a string column"),
        }
    }
}

/// A deterministic xorshift64* stream, so every run benchmarks the same events.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// Skews towards small values: the square of a uniform draw.
    pub fn skewed(&mut self, n: u64) -> u64 {
        let u = self.below(1 << 20);
        (u * u * n) >> 40
    }
}

const WORDS: [&str; 64] = [
    "the", "of", "and", "to", "in", "is", "that", "it", "for", "was", "on", "are", "with", "as", "this", "be", "at",
    "have", "from", "or", "one", "had", "by", "word", "but", "not", "what", "all", "were", "we", "when", "your", "can",
    "said", "there", "use", "each", "which", "she", "how", "their", "will", "other", "about", "out", "many", "then",
    "them", "these", "some", "her", "would", "make", "like", "him", "into", "time", "has", "look", "two", "more",
    "write", "see", "number",
];

fn sentence(rng: &mut Rng) -> String {
    let words = 6 + rng.below(30);
    let mut text = String::new();
    for i in 0..words {
        if i > 0 {
            text.push(' ');
        }
        text.push_str(WORDS[rng.skewed(WORDS.len() as u64) as usize]);
    }
    text
}

/// Chat traffic as a messaging app sees it: a few very busy rooms and many quiet ones, activity in bursts, a quarter
/// of messages replying into a thread, and a stream of edits, reactions and redactions alongside the messages.
fn generate_chat(rows: usize) -> Dataset {
    let mut rng = Rng::new(0xC4A7 ^ rows as u64);
    let rooms = (rows / 200).max(50) as u64;
    let users = (rows / 20).max(100) as u64;
    let mut event_id = Vec::with_capacity(rows);
    let mut occurred_at = Vec::with_capacity(rows);
    let mut entity_id = Vec::with_capacity(rows);
    let mut event_type = Vec::with_capacity(rows);
    let mut source = Vec::with_capacity(rows);
    let mut thread_root = Vec::with_capacity(rows);
    let mut sender = Vec::with_capacity(rows);
    let mut body = Vec::with_capacity(rows);
    let mut msgtype = Vec::with_capacity(rows);
    let mut length = Vec::with_capacity(rows);
    let mut recent: std::collections::HashMap<u64, Vec<usize>> = std::collections::HashMap::new();
    let mut now = FIRST_OCCURRED_AT;
    for i in 0..rows {
        now += if rng.below(50) == 0 {
            2_000_000_000 + rng.below(30_000_000_000) as i64
        } else {
            1_000_000 + rng.below(4_000_000) as i64
        };
        let outer = rng.skewed(rooms);
        let room = rng.skewed(outer + 1);
        let kind = rng.below(100);
        let (kind, root) = if kind < 80 {
            let history = recent.entry(room).or_default();
            let root = if !history.is_empty() && rng.below(4) == 0 {
                Some(history[rng.below(history.len() as u64) as usize])
            } else {
                None
            };
            if root.is_none() {
                history.push(i);
                if history.len() > 20 {
                    history.remove(0);
                }
            }
            ("m.room.message", root)
        } else if kind < 90 {
            ("m.reaction", None)
        } else if kind < 97 {
            ("m.room.message.edit", None)
        } else {
            ("m.room.redaction", None)
        };
        event_id.push(format!("{:032x}", 0xA000u128 + i as u128));
        occurred_at.push(now);
        entity_id.push(format!("!room{room:06}:example.org"));
        event_type.push(kind.to_owned());
        source.push(["web", "ios", "android", "bridge"][rng.below(4) as usize].to_owned());
        thread_root.push(root.map(|r| format!("{:032x}", 0xA000u128 + r as u128)).unwrap_or_default());
        sender.push(format!("@user{}:example.org", rng.skewed(users)));
        let text = if kind == "m.room.message" || kind == "m.room.message.edit" {
            sentence(&mut rng)
        } else {
            String::new()
        };
        length.push(text.len() as i64);
        body.push(text);
        msgtype.push(["m.text", "m.text", "m.text", "m.text", "m.text", "m.text", "m.text", "m.text", "m.image", "m.notice"][rng.below(10) as usize].to_owned());
    }
    Dataset {
        columns: vec![
            ("event_id".to_owned(), Column::Str(event_id)),
            ("occurred_at".to_owned(), Column::I64(occurred_at)),
            ("entity_id".to_owned(), Column::Str(entity_id)),
            ("event_type".to_owned(), Column::Str(event_type)),
            ("source".to_owned(), Column::Str(source)),
            ("thread_root".to_owned(), Column::Str(thread_root)),
            ("payload.body".to_owned(), Column::Str(body)),
            ("payload.length".to_owned(), Column::I64(length)),
            ("payload.msgtype".to_owned(), Column::Str(msgtype)),
            ("payload.sender".to_owned(), Column::Str(sender)),
        ],
        profile: Profile::Chat,
        rows,
    }
}

pub fn generate(profile: Profile, rows: usize) -> Dataset {
    if profile == Profile::Chat {
        return generate_chat(rows);
    }
    let mut rng = Rng::new(0x0E_0E_0E_0E ^ rows as u64);
    let entities = (rows / 40).max(100) as u64;
    let (types, sources): (&[&str], &[&str]) = match profile {
        Profile::Chat => unreachable!("chat has its own generator"),
        Profile::Messages => (
            &["msg.text", "msg.text", "msg.text", "msg.text", "msg.edit", "msg.react", "msg.join", "msg.redact"],
            &["web", "ios", "android", "bridge"],
        ),
        Profile::Telemetry => (
            &[
                "metric.cpu", "metric.cpu", "metric.cpu", "metric.mem", "metric.mem", "metric.disk", "metric.net",
                "metric.fault",
            ],
            &["agent", "gateway", "sensor", "batch"],
        ),
    };
    let mut event_id = Vec::with_capacity(rows);
    let mut occurred_at = Vec::with_capacity(rows);
    let mut entity_id = Vec::with_capacity(rows);
    let mut event_type = Vec::with_capacity(rows);
    let mut source = Vec::with_capacity(rows);
    let mut now = FIRST_OCCURRED_AT;
    for i in 0..rows {
        now += 500_000 + rng.below(1_000_000) as i64;
        event_id.push(format!("{:032x}", 0xA000u128 + i as u128));
        occurred_at.push(now);
        entity_id.push(match profile {
            Profile::Chat => unreachable!(),
            Profile::Messages => format!("room-{:06}", rng.skewed(entities)),
            Profile::Telemetry => format!("dev-{:06}", rng.below(entities)),
        });
        event_type.push(types[rng.below(types.len() as u64) as usize].to_owned());
        source.push(sources[rng.below(sources.len() as u64) as usize].to_owned());
    }
    let mut columns = vec![
        ("event_id".to_owned(), Column::Str(event_id)),
        ("occurred_at".to_owned(), Column::I64(occurred_at)),
        ("entity_id".to_owned(), Column::Str(entity_id)),
        ("event_type".to_owned(), Column::Str(event_type)),
        ("source".to_owned(), Column::Str(source)),
    ];
    match profile {
        Profile::Chat => unreachable!(),
        Profile::Messages => {
            let mut user_id = Vec::with_capacity(rows);
            let mut room = Vec::with_capacity(rows);
            let mut body = Vec::with_capacity(rows);
            let mut lang = Vec::with_capacity(rows);
            let mut length = Vec::with_capacity(rows);
            for _ in 0..rows {
                user_id.push(1_000 + rng.skewed(2_000_000) as i64);
                room.push(format!("!room{}:example.org", rng.skewed(500)));
                let text = sentence(&mut rng);
                length.push(text.len() as i64);
                body.push(text);
                lang.push(["en", "en", "en", "de", "fr", "es", "ja", "pt"][rng.below(8) as usize].to_owned());
            }
            columns.push(("payload.body".to_owned(), Column::Str(body)));
            columns.push(("payload.lang".to_owned(), Column::Str(lang)));
            columns.push(("payload.length".to_owned(), Column::I64(length)));
            columns.push(("payload.room".to_owned(), Column::Str(room)));
            columns.push(("payload.user_id".to_owned(), Column::I64(user_id)));
        }
        Profile::Telemetry => {
            let mut reading = Vec::with_capacity(rows);
            let mut battery = Vec::with_capacity(rows);
            let mut firmware = Vec::with_capacity(rows);
            let mut region = Vec::with_capacity(rows);
            let mut error_code = Vec::with_capacity(rows);
            let mut level = 50.0f64;
            let mut charge = 100i64;
            for _ in 0..rows {
                level = (level + (rng.below(2001) as f64 - 1000.0) / 500.0).clamp(0.0, 100.0);
                reading.push((level * 100.0).round() / 100.0);
                if rng.below(8) == 0 {
                    charge = (charge - 1).max(0);
                }
                battery.push(charge);
                firmware.push(format!("v{}.{}", 1 + rng.below(3), rng.below(2)));
                region.push(format!("region-{:02}", rng.below(24)));
                error_code.push(if rng.below(100) == 0 { 1 + rng.below(40) as i64 } else { 0 });
            }
            columns.push(("payload.battery".to_owned(), Column::I64(battery)));
            columns.push(("payload.error_code".to_owned(), Column::I64(error_code)));
            columns.push(("payload.firmware".to_owned(), Column::Str(firmware)));
            columns.push(("payload.reading".to_owned(), Column::F64(reading)));
            columns.push(("payload.region".to_owned(), Column::Str(region)));
        }
    }
    Dataset {
        columns,
        profile,
        rows,
    }
}
