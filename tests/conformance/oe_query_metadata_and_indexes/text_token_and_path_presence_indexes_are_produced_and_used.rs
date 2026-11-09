use oe::indexes::path_presence::PathPresenceIndex;
use oe::indexes::text_token::TextTokenIndex;
use oe::indexes::{Exactness, SkipIndexKind};

#[test]
fn token_query_pruned_then_confirmed() {
    let values = ["error rate exceeded threshold", "payment processed successfully"];
    let index = TextTokenIndex::build_from_values(&values, false);

    assert_eq!(
        SkipIndexKind::TextToken.default_exactness(),
        Exactness::InexactNoFalseNegative,
        "TextToken index is inexact — the exact check must be retained above the scan"
    );

    assert!(
        index.might_contain_token("error"),
        "a token present in the granule must survive pruning"
    );
    assert!(
        index.might_contain_token("payment"),
        "a token present in the granule must survive pruning"
    );

    let pruned = !index.might_contain_token("zxqvwm");
    let _ = pruned;

    let bytes = index.encode();
    let decoded = TextTokenIndex::decode(&bytes).expect("round-trip must succeed");
    assert!(decoded.might_contain_token("error"));
    assert!(decoded.might_contain_token("payment"));
}

#[test]
fn contains_query_pruned_then_confirmed() {
    let values = ["error rate exceeded threshold", "payment processed successfully"];
    let index = TextTokenIndex::build_from_values(&values, true);
    assert!(
        index.has_ngrams(),
        "a substring-searchable field carries the n-gram layer"
    );

    assert!(
        index.might_contain_substring("rate exce"),
        "a substring the granule holds must survive pruning"
    );
    assert!(
        index.might_contain_substring("processed"),
        "a substring the granule holds must survive pruning"
    );

    assert!(
        !index.might_contain_substring("zxqvwm"),
        "a substring whose trigrams are absent must prune the granule"
    );

    let tokens_only = TextTokenIndex::build_from_values(&values, false);
    assert!(
        tokens_only.might_contain_substring("zxqvwm"),
        "without n-grams the filter must never prune a substring predicate"
    );

    let decoded = TextTokenIndex::decode(&index.encode()).expect("round-trip must succeed");
    assert!(decoded.might_contain_substring("rate exce"));
    assert!(!decoded.might_contain_substring("zxqvwm"));
}

#[test]
fn rare_payload_path_restricts_the_read() {
    let paths_granule_a = ["attributes.revenue.amount", "attributes.currency", "context.source"];
    let paths_granule_b = ["context.source", "user.id"];

    let index_a = PathPresenceIndex::build_from_paths(&paths_granule_a);
    let index_b = PathPresenceIndex::build_from_paths(&paths_granule_b);

    let rare_path = "attributes.revenue.amount";

    assert!(
        index_a.might_contain_path(rare_path),
        "granule A contains the path and must survive pruning"
    );

    assert!(
        index_b.might_contain_path("context.source"),
        "context.source is present in granule B and must not be dropped"
    );

    assert_eq!(
        SkipIndexKind::PathPresence.default_exactness(),
        Exactness::InexactNoFalseNegative,
        "PathPresence index is inexact — the exact check must be retained"
    );

    let bytes_a = index_a.encode();
    let decoded_a = PathPresenceIndex::decode(&bytes_a).expect("round-trip must succeed");
    assert!(decoded_a.might_contain_path(rare_path));

    let bytes_b = index_b.encode();
    let decoded_b = PathPresenceIndex::decode(&bytes_b).expect("round-trip must succeed");
    assert!(decoded_b.might_contain_path("user.id"));
}
